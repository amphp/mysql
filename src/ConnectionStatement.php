<?php

namespace Amp\Mysql;

use Amp\Deferred;
use Amp\Promise;
use Amp\Sql\ConnectionException;
use Amp\Sql\FailureException;
use Amp\Success;
use function Amp\call;

final class ConnectionStatement implements Statement
{
    private $paramCount;
    private $numParamCount;
    private $named = [];
    private $byNamed;
    private $query;
    private $stmtId;
    private $prebound = [];

    /** @var Internal\Processor|null */
    private $processor;

    /** @var Internal\ResultProxy */
    private $result;

    /** @var int */
    private $lastUsedAt;

    public function __construct(Internal\Processor $processor, string $query, int $stmtId, array $named, Internal\ResultProxy $result)
    {
        $this->processor = $processor;
        $this->query = $query;
        $this->stmtId = $stmtId;
        $this->result = $result;
        $this->numParamCount = $this->paramCount = $this->result->columnsToFetch;
        $this->byNamed = $named;

        foreach ($named as $name => $ids) {
            foreach ($ids as $id) {
                $this->named[$id] = $name;
                $this->numParamCount--;
            }
        }

        $this->lastUsedAt = \time();
    }

    private function getProcessor(): Internal\Processor
    {
        if ($this->processor === null) {
            throw new \Error("The statement has been closed");
        }

        if (!$this->processor->isAlive()) {
            throw new ConnectionException("Connection went away");
        }

        return $this->processor;
    }

    public function isAlive(): bool
    {
        if ($this->processor === null) {
            return false;
        }

        return $this->processor->isAlive();
    }

    /** {@inheritdoc} */
    public function bind($paramId, $data): void
    {
        if (\is_int($paramId)) {
            if ($paramId >= $this->numParamCount) {
                throw new \Error("Parameter id $paramId is not defined for this prepared statement");
            }
            $i = $paramId;
        } elseif (\is_string($paramId)) {
            if (!isset($this->byNamed[$paramId])) {
                throw new \Error("Parameter :$paramId is not defined for this prepared statement");
            }
            $array = $this->byNamed[$paramId];
            $i = \reset($array);
        } else {
            throw new \TypeError("Invalid parameter ID type");
        }

        if (!\is_scalar($data) && !(\is_object($data) && \method_exists($data, '__toString'))) {
            throw new \TypeError("Data must be scalar or object that implements __toString method");
        }

        do {
            $realId = -1;
            while (isset($this->named[++$realId]) || $i-- > 0) {
                if (!\is_numeric($paramId) && isset($this->named[$realId]) && $this->named[$realId] == $paramId) {
                    break;
                }
            }

            $this->getProcessor()->bindParam($this->stmtId, $realId, $data);
        } while (isset($array) && $i = \next($array));

        if (isset($this->prebound[$paramId])) {
            $this->prebound[$paramId] .= (string) $data;
        } else {
            $this->prebound[$paramId] = (string) $data;
        }
    }

    /** {@inheritdoc} */
    public function execute(array $params = []): Promise
    {
        $this->lastUsedAt = \time();

        $prebound = $args = [];
        for ($unnamed = $i = 0; $i < $this->paramCount; $i++) {
            if (isset($this->named[$i])) {
                $name = $this->named[$i];
                if (\array_key_exists($name, $params)) {
                    $args[$i] = $params[$name];
                } elseif (!\array_key_exists($name, $this->prebound)) {
                    throw new \Error("Named parameter '$name' missing for executing prepared statement");
                } else {
                    $prebound[$i] = $this->prebound[$name];
                }
            } elseif (\array_key_exists($unnamed, $params)) {
                $args[$i] = $params[$unnamed];
                $unnamed++;
            } elseif (!\array_key_exists($unnamed, $this->prebound)) {
                throw new \Error("Parameter $unnamed for prepared statement missing");
            } else {
                $prebound[$i] = $this->prebound[$unnamed++];
            }
        }

        return call(function () use ($prebound, $args) {
            $result = yield $this->getProcessor()
                ->execute($this->stmtId, $this->query, $this->result->params, $prebound, $args);

            if ($result instanceof Internal\ResultProxy) {
                $result = new ConnectionResultSet($result);
                return $result;
            }

            if ($result instanceof CommandResult) {
                return $result;
            }

            throw new FailureException("Unrecognized result type");
        });
    }

    public function getQuery(): string
    {
        return $this->query;
    }

    private function close(): void
    {
        if ($this->processor === null) {
            return;
        }

        $this->processor->closeStmt($this->stmtId);
        $this->processor->unreference();
        $this->processor = null;
    }

    public function reset(): Promise
    {
        return $this->getProcessor()->resetStmt($this->stmtId);
    }

    public function getFields(): Promise
    {
        if ($this->result->state >= Internal\ResultProxy::COLUMNS_FETCHED) {
            return new Success($this->result->columns);
        }

        if (isset($this->result->deferreds[Internal\ResultProxy::COLUMNS_FETCHED][0])) {
            return $this->result->deferreds[Internal\ResultProxy::COLUMNS_FETCHED][0][0]->promise();
        }

        $deferred = new Deferred;
        $this->result->deferreds[Internal\ResultProxy::COLUMNS_FETCHED][0] = [$deferred, &$this->result->columns, null];
        return $deferred->promise();
    }

    /** {@inheritdoc} */
    public function getLastUsedAt(): int
    {
        return $this->lastUsedAt;
    }

    public function __destruct()
    {
        $this->close();
    }
}
