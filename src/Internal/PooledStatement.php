<?php

namespace Amp\Mysql\Internal;

use Amp\Loop;
use Amp\Mysql\Operation;
use Amp\Mysql\Pool;
use Amp\Mysql\Statement;
use Amp\Promise;
use Amp\Success;
use function Amp\call;

final class PooledStatement implements Statement {
    /** @var \Amp\Mysql\Pool */
    private $pool;

    /** @var \SplQueue */
    private $statements;

    /** @var string */
    private $sql;

    /** @var int */
    private $lastUsedAt;

    /** @var string */
    private $timeoutWatcher;

    /** @var mixed[] */
    private $boundParams = [];

    /** @var callable */
    private $prepare;

    /**
     * @param \Amp\Mysql\Pool $pool Pool used to re-create the statement if the original closes.
     * @param \Amp\Mysql\Statement $statement
     * @param callable $prepare
     */
    public function __construct(Pool $pool, Statement $statement, callable $prepare) {
        $this->lastUsedAt = \time();
        $this->statements = $statements = new \SplQueue;
        $this->pool = $pool;
        $this->prepare = $prepare;
        $this->sql = $statement->getQuery();

        $this->statements->push($statement);

        $this->timeoutWatcher = Loop::repeat(1000, static function () use ($pool, $statements) {
            $now = \time();
            $idleTimeout = $pool->getIdleTimeout();

            while (!$statements->isEmpty()) {
                /** @var \Amp\Mysql\Statement $statement */
                $statement = $statements->bottom();

                if ($statement->lastUsedAt() + $idleTimeout > $now) {
                    return;
                }

                $statements->shift();
            }
        });

        Loop::unreference($this->timeoutWatcher);
    }

    public function __destruct() {
        Loop::cancel($this->timeoutWatcher);
    }

    /**
     * {@inheritdoc}
     *
     * Unlike regular statements, as long as the pool is open this statement will not die.
     */
    public function execute(array $params = []): Promise {
        $this->lastUsedAt = \time();

        return call(function () use ($params) {
            /** @var \Amp\Mysql\Statement $statement */
            $statement = yield from $this->pop();

            yield $statement->reset();

            try {
                foreach ($this->boundParams as $param => $data) {
                    $statement->bind($param, $data);
                }

                $result = yield $statement->execute($params);
            } catch (\Throwable $exception) {
                $this->statements->push($statement);
                throw $exception;
            }

            if ($result instanceof Operation) {
                $result->onDestruct(function () use ($statement) {
                    $this->statements->push($statement);
                });
            } else {
                $this->statements->push($statement);
            }

            return $result;
        });
    }

    /** {@inheritdoc} */
    public function bind($paramId, $data) {
        if (!\is_int($paramId) && !\is_string($paramId)) {
            throw new \TypeError("Invalid parameter ID type");
        }

        $this->boundParams[$paramId] = $data;
    }

    /** {@inheritdoc} */
    public function isAlive(): bool {
        return $this->pool->isAlive();
    }

    /** {@inheritdoc} */
    public function getQuery(): string {
        return $this->sql;
    }

    /** {@inheritdoc} */
    public function getFields(): Promise {
        return call(function () {
            /** @var \Amp\Mysql\Statement $statement */
            $statement = yield from $this->pop();
            return yield $statement->getFields();
        });
    }

    /** {@inheritdoc} */
    public function reset(): Promise {
        $this->boundParams = [];
        return new Success;
    }

    /** {@inheritdoc} */
    public function lastUsedAt(): int {
        return $this->lastUsedAt;
    }

    private function pop(): \Generator {
        if (!$this->statements->isEmpty()) {
            do {
                /** @var \Amp\Mysql\Statement $statement */
                $statement = $this->statements->shift();
            } while (!$statement->isAlive() && !$this->statements->isEmpty());
        } else {
            $statement = yield ($this->prepare)($this->sql);
        }

        return $statement;
    }
}
