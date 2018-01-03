<?php

namespace Amp\Mysql;

use Amp\Deferred;
use Amp\Loop;
use Amp\Promise;
use function Amp\call;

class Pool implements Link {
    const DEFAULT_MAX_CONNECTIONS = 100;
    const DEFAULT_IDLE_TIMEOUT = 60;

    /** @var \Amp\Mysql\Internal\ConnectionConfig */
    private $config;

    /** @var int */
    private $maxConnections;

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

    /** @var string */
    private $timeoutWatcher;

    /** @var int */
    private $idleTimeout = self::DEFAULT_IDLE_TIMEOUT;

    /**
     * @internal Use \Amp\Mysql\pool() instead.
     *
     * @param \Amp\Mysql\Internal\ConnectionConfig $config
     * @param int $maxConnections
     *
     * @throws \Error If $maxConnections is less than 1.
     */
    public function __construct(Internal\ConnectionConfig $config, int $maxConnections = self::DEFAULT_MAX_CONNECTIONS) {
        $this->config = $config;
        $this->maxConnections = $maxConnections;
        if ($this->maxConnections < 1) {
            throw new \Error("Pool must contain at least one connection");
        }

        $this->connections = new \SplObjectStorage;
        $this->idle = new \SplQueue;

        $this->connections = $connections = new \SplObjectStorage;
        $this->idle = $idle = new \SplQueue;

        $idleTimeout = &$this->idleTimeout;

        $this->timeoutWatcher = Loop::repeat(1000, static function () use (&$idleTimeout, $connections, $idle) {
            $now = \time();
            while (!$idle->isEmpty()) {
                /** @var \Amp\Mysql\Connection $connection */
                $connection = $idle->bottom();

                if ($connection->lastUsedAt() + $idleTimeout > $now) {
                    return;
                }

                // Close connection and remove it from the pool.
                $idle->shift();
                $connections->detach($connection);
                $connection->close();
            }
        });

        Loop::unreference($this->timeoutWatcher);
    }

    public function close() {
        $this->closed = true;
        foreach ($this->connections as $connection) {
            $connection->close();
        }
        $this->idle = new \SplQueue;
        $this->connections = new \SplObjectStorage;
    }

    /**
     * @param int $timeout The maximum number of seconds a connection may be idle before being closed and removed
     *     from the pool. Use 0 to disable automatic connection removal.
     *
     * @throws \Error If the timeout is less than 0.
     */
    public function setIdleTimeout(int $timeout) {
        if ($timeout < 0) {
            throw new \Error("Timeout must be greater than or equal to 0");
        }

        $this->idleTimeout = $timeout;

        if ($this->idleTimeout > 0) {
            Loop::enable($this->timeoutWatcher);
        } else {
            Loop::disable($this->timeoutWatcher);
        }
    }

    /**
     * @return \Amp\Promise<\Amp\Mysql\Connection>
     *
     * @throws \Amp\Mysql\FailureException
     */
    protected function createConnection(): Promise {
        return Connection::connect($this->config);
    }

    /**
     * @return int Maximum number of connections.
     */
    public function getMaxConnections(): int {
        return $this->maxConnections;
    }

    /**
     * @return int Current number of connections in the pool.
     */
    public function getConnectionCount(): int {
        return $this->connections->count();
    }

    /**
     * @return int Current number of idle connections in the pool.
     */
    public function getIdleConnectionCount(): int {
        return $this->idle->count();
    }

    /**
     * Extracts an idle connection from the pool. The connection is completely removed from the pool and cannot be
     * put back into the pool. Useful for operations where connection state must be changed.
     *
     * @return \Amp\Promise<\Amp\Mysql\Connection>
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
                    if (!$connection instanceof Connection) {
                        throw new \Error(\sprintf(
                            "%s::createConnection() must resolve to an instance of %s",
                            static::class,
                            Connection::class
                        ));
                    }
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
