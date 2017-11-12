<?php

namespace Amp\Mysql;

use Amp\Deferred;
use Amp\Promise;
use Amp\Success;

class Statement {
    private $paramCount;
    private $numParamCount;
    private $named = [];
    private $byNamed;
    private $query;
    private $stmtId;
    private $prebound = [];

    /** @var \Amp\Mysql\Internal\CompletionQueue */
    private $queue;

    /** @var \Amp\Mysql\Internal\Processor */
    private $processor; // when doing something on $processor, it must be checked if still same connection, else throw Exception! @TODO {or redo query, fetch???}

    /** @var \Amp\Mysql\Internal\ResultProxy */
    private $result;

    public function __construct(Internal\Processor $processor, string $query, int $stmtId, array $named, Internal\ResultProxy $result) {
        $this->processor = $processor;
        $this->query = $query;
        $this->stmtId = $stmtId;
        $this->result = $result;
        $this->numParamCount = $this->paramCount = $this->result->columnsToFetch;
        $this->byNamed = $named;

        $this->queue = new Internal\CompletionQueue;

        $this->queue->onComplete([$this->processor, 'delRef']);

        foreach ($named as $name => $ids) {
            foreach ($ids as $id) {
                $this->named[$id] = $name;
                $this->numParamCount--;
            }
        }
    }

    private function getProcessor(): Internal\Processor {
        if (!$this->processor->isAlive()) {
            throw new ConnectionException("Connection went away, no way provided to restore connection via callable in ConnectionConfig::ready");
        }

        return $this->processor;
    }

    public function onComplete(callable $onComplete) {
        $this->queue->onComplete($onComplete);
    }

    /**
     * @param int|string $paramId Parameter ID or name.
     * @param mixed $data Data to bind to parameter.
     *
     * @throws \Exception
     * @throws \TypeError
     */
    public function bind($paramId, $data) {
        if (is_numeric($paramId)) {
            if ($paramId >= $this->numParamCount) {
                throw new \Error("Parameter id $paramId is not defined for this prepared statement");
            }
            $i = $paramId;
        } else {
            if (!isset($this->byNamed[$paramId])) {
                throw new \Error("Parameter :$paramId is not defined for this prepared statement");
            }
            $array = $this->byNamed[$paramId];
            $i = reset($array);
        }

        if (!is_scalar($data) && !(is_object($data) && method_exists($data, '__toString'))) {
            throw new \TypeError("Data must be scalar or object that implements __toString method");
        }

        do {
            $realId = -1;
            while (isset($this->named[++$realId]) || $i-- > 0) {
                if (!is_numeric($paramId) && $this->named[$realId] == $paramId) {
                    break;
                }
            }

            $this->getProcessor()->bindParam($this->stmtId, $realId, $data);
        } while (isset($array) && $i = next($array));

        if (isset($this->prebound[$paramId])) {
            $this->prebound[$paramId] .= $data;
        } else {
            $this->prebound[$paramId] = $data;
        }
    }

    /**
     * @param mixed ...$data Data to bind to parameters.
     *
     * @return \Amp\Promise
     */
    public function execute(...$data): Promise {
        $prebound = $args = [];
        for ($unnamed = $i = 0; $i < $this->paramCount; $i++) {
            if (isset($this->named[$i])) {
                $name = $this->named[$i];
                if (array_key_exists($name, $data) && $data[$name] !== []) {
                    if (\is_array($data[$name])) {
                        $args[$i] = reset($data[$name]);
                        unset($data[$name][key($data[$name])]);
                    } else {
                        $args[$i] = $data[$name];
                        unset($data[$name]);
                    }
                } elseif (!isset($this->prebound[$name])) {
                    if ($data[$name] === []) {
                        throw new \Error("Named parameter $name is not providing enough elements");
                    } else {
                        throw new \Error("Named parameter $name missing for executing prepared statement");
                    }
                } else {
                    $prebound[$i] = $this->prebound[$name];
                }
            } elseif (array_key_exists($unnamed, $data)) {
                $args[$i] = $data[$unnamed];
                $unnamed++;
            } elseif (!isset($this->prebound[$unnamed])) {
                throw new \Error("Parameter $unnamed for prepared statement missing");
            } else {
                $prebound[$i] = $this->prebound[$unnamed++];
            }
        }

        return $this->getProcessor()->execute($this->stmtId, $this->query, $this->result->params, $prebound, $args);
    }

    public function close() {
        $this->processor->closeStmt($this->stmtId);
        $this->queue->complete();
    }

    public function reset() {
        $this->getProcessor()->resetStmt($this->stmtId);
    }

    // @TODO not necessary, see cursor?!
    public function fetch(): Promise {
        return $this->getProcessor()->fetchStmt($this->stmtId);
    }

    public function getFields(): Promise {
        if ($this->result->state >= Internal\ResultProxy::COLUMNS_FETCHED) {
            return new Success($this->result->columns);
        } elseif (isset($this->result->deferreds[Internal\ResultProxy::COLUMNS_FETCHED][0])) {
            return $this->result->deferreds[Internal\ResultProxy::COLUMNS_FETCHED][0][0]->promise();
        } else {
            $deferred = new Deferred;
            $this->result->deferreds[Internal\ResultProxy::COLUMNS_FETCHED][0] = [$deferred, &$this->result->columns, null];
            return $deferred->promise();
        }
    }

    public function connInfo(): ConnectionState {
        return $this->getProcessor()->getConnInfo();
    }

    public function __destruct() {
        $this->close();
    }
}
