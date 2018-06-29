<?php

namespace Amp\Mysql\Internal;

use Amp\Loop;
use Amp\Mysql\Pool;
use Amp\Mysql\Statement;
use Amp\Promise;
use Amp\Sql\Operation;
use Amp\Success;
use function Amp\call;

final class PooledStatement implements Statement {
    /** @var Pool */
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
     * @param Pool $pool Pool used to re-create the statement if the original closes.
     * @param Statement $statement
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
            $idleTimeout = ((int) ($pool->getIdleTimeout() / 10)) ?: 1;

            while (!$statements->isEmpty()) {
                /** @var Statement $statement */
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
            /** @var Statement $statement */
            $statement = yield from $this->pop();

            yield $statement->reset();

            try {
                foreach ($this->boundParams as $param => $data) {
                    $statement->bind($param, $data);
                }

                $result = yield $statement->execute($params);
            } catch (\Throwable $exception) {
                $this->push($statement);
                throw $exception;
            }

            if ($result instanceof Operation) {
                $result->onDestruct(function () use ($statement) {
                    $this->push($statement);
                });
            } else {
                $this->push($statement);
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
            /** @var Statement $statement */
            $statement = yield from $this->pop();

            try {
                return yield $statement->getFields();
            } finally {
                $this->push($statement);
            }
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

    /**
     * Only retains statements if less than 10% of the pool is consumed by this statement and the pool has
     * available connections.
     *
     * @param Statement $statement
     */
    private function push(Statement $statement) {
        $maxConnections = $this->pool->getMaxConnections();

        if ($this->statements->count() > ($maxConnections / 10)) {
            return;
        }

        if ($maxConnections === $this->pool->getConnectionCount() && $this->pool->getIdleConnectionCount() === 0) {
            return;
        }

        $this->statements->push($statement);
    }

    /**
     * Get a statement from the idle queue of statements or create a new statement from the pool.
     */
    private function pop(): \Generator {
        if (!$this->statements->isEmpty()) {
            do {
                /** @var Statement $statement */
                $statement = $this->statements->shift();
            } while (!$statement->isAlive() && !$this->statements->isEmpty());
        } else {
            $statement = yield ($this->prepare)($this->sql);
        }

        return $statement;
    }
}
