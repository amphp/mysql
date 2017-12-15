<?php

namespace Amp\Mysql;

use Amp\Deferred;
use Amp\Promise;
use function Amp\call;

abstract class AbstractPool implements Pool {
    /** @var \SplQueue */
    private $idle;

    /** @var \SplObjectStorage */
    private $connections;

    /** @var \Amp\Promise|null */
    private $promise;

    /** @var int Number of pending connections. */
    private $pending = 0;

    /** @var \Amp\Deferred|null */
    private $deferred;

    /** @var bool */
    private $closed = false;

    /**
     * @return \Amp\Promise<\Amp\Mysql\Connection>
     *
     * @throws \Amp\Mysql\FailureException
     */
    abstract protected function createConnection(): Promise;

    public function __construct() {
        $this->connections = new \SplObjectStorage;
        $this->idle = new \SplQueue;
    }

    public function close() {
        $this->closed = true;
        foreach ($this->connections as $connection) {
            $connection->close();
        }
        $this->connections = new \SplObjectStorage;
    }

    /**
     * {@inheritdoc}
     */
    public function getConnectionCount(): int {
        return $this->connections->count();
    }

    /**
     * {@inheritdoc}
     */
    public function getIdleConnectionCount(): int {
        return $this->idle->count();
    }

    /**
     * @param \Amp\Mysql\Connection $connection
     *
     * @throws \Error If the pool has been closed, the connection exists in the pool, or the connection is dead.
     */
    protected function addConnection(Connection $connection) {
        if ($this->closed) {
            throw new \Error("The pool has been closed");
        }

        if (isset($this->connections[$connection])) {
            throw new \Error("Connection is already a part of this pool");
        }

        if (!$connection->isAlive()) {
            throw new \Error("The connection is dead");
        }

        $this->connections->attach($connection);
        $this->idle->push($connection);

        if ($this->deferred instanceof Deferred) {
            $this->deferred->resolve($connection);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function extractConnection(): Promise {
        return call(function () {
            $connection = yield from $this->pop();
            $this->connections->detach($connection);
            return $connection;
        });
    }

    /**
     * @return \Generator
     *
     * @resolve \Amp\Postgres\Connection
     *
     * @throws \Amp\Mysql\FailureException If creating a connection fails.
     * @throws \Error If the pool has been closed.
     */
    private function pop(): \Generator {
        if ($this->closed) {
            throw new \Error("The pool has been closed");
        }

        while ($this->promise !== null && $this->connections->count() + $this->pending >= $this->getMaxConnections()) {
            yield $this->promise; // Prevent simultaneous connection creation when connection count is at maximum - 1.
        }

        while ($this->idle->isEmpty()) { // While loop to ensure an idle connection is available after promises below are resolved.
            if ($this->connections->count() + $this->pending >= $this->getMaxConnections()) {
                // All possible connections busy, so wait until one becomes available.
                try {
                    $this->deferred = new Deferred;
                    yield $this->promise = $this->deferred->promise(); // May be resolved with defunct connection.
                } finally {
                    $this->deferred = null;
                    $this->promise = null;
                }
            } else {
                // Max connection count has not been reached, so open another connection.
                ++$this->pending;
                try {
                    $connection = yield $this->createConnection();
                } finally {
                    --$this->pending;
                }

                $this->connections->attach($connection);
                return $connection;
            }
        }

        // Shift a connection off the idle queue.
        return $this->idle->shift();
    }

    /**
     * @param \Amp\Mysql\Connection $connection
     *
     * @throws \Error If the connection is not part of this pool.
     */
    private function push(Connection $connection) {
        if ($this->closed) {
            return;
        }

        \assert(isset($this->connections[$connection]), 'Connection is not part of this pool');

        if ($connection->isAlive()) {
            $this->idle->push($connection);
        } else {
            $this->connections->detach($connection);
        }

        if ($this->deferred instanceof Deferred) {
            $this->deferred->resolve($connection);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function query(string $sql): Promise {
        return call(function () use ($sql) {
            /** @var \Amp\Mysql\Connection $connection */
            $connection = yield from $this->pop();

            try {
                $result = yield $connection->query($sql);
            } catch (\Throwable $exception) {
                $this->push($connection);
                throw $exception;
            }

            if ($result instanceof Operation) {
                $result->onDestruct(function () use ($connection) {
                    $this->push($connection);
                });
            } else {
                $this->push($connection);
            }

            return $result;
        });
    }

    /**
     * {@inheritdoc}
     */
    public function prepare(string $sql): Promise {
        return call(function () use ($sql) {
            /** @var \Amp\Mysql\Connection $connection */
            $connection = yield from $this->pop();

            try {
                /** @var \Amp\Mysql\Statement $statement */
                $statement = yield $connection->prepare($sql);
            } catch (\Throwable $exception) {
                $this->push($connection);
                throw $exception;
            }

            $statement->onDestruct(function () use ($connection) {
                $this->push($connection);
            });

            return $statement;
        });
    }

    /**
     * {@inheritdoc}
     */
    public function execute(string $sql, array $params = []): Promise {
        return call(function () use ($sql, $params) {
            /** @var \Amp\Mysql\Connection $connection */
            $connection = yield from $this->pop();

            try {
                $result = yield $connection->execute($sql, $params);
            } catch (\Throwable $exception) {
                $this->push($connection);
                throw $exception;
            }

            if ($result instanceof Operation) {
                $result->onDestruct(function () use ($connection) {
                    $this->push($connection);
                });
            } else {
                $this->push($connection);
            }

            return $result;
        });
    }

    /**
     * {@inheritdoc}
     */
    public function transaction(int $isolation = Transaction::COMMITTED): Promise {
        return call(function () use ($isolation) {
            /** @var \Amp\Mysql\Connection $connection */
            $connection = yield from $this->pop();

            try {
                /** @var \Amp\Mysql\Transaction $transaction */
                $transaction = yield $connection->transaction($isolation);
            } catch (\Throwable $exception) {
                $this->push($connection);
                throw $exception;
            }

            $transaction->onDestruct(function () use ($connection) {
                $this->push($connection);
            });

            return $transaction;
        });
    }
}
