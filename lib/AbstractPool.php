<?php

namespace Amp\Mysql;

use Amp\Coroutine;
use Amp\Deferred;
use Amp\Promise;

abstract class AbstractPool implements Pool {
    /** @var \SplQueue */
    private $idle;

    /** @var \SplQueue */
    private $busy;

    /** @var \SplObjectStorage */
    private $connections;

    /** @var \Amp\Promise|null */
    private $promise;

    /** @var \Amp\Deferred|null */
    private $deferred;

    /**
     * @return \Amp\Promise<\Amp\Mysql\Connection>
     *
     * @throws \Amp\Mysql\FailureException
     */
    abstract protected function createConnection(): Promise;

    public function __construct() {
        $this->connections = new \SplObjectStorage;
        $this->idle = new \SplQueue;
        $this->busy = new \SplQueue;
    }

    public function close() {
        foreach ($this->connections as $connection) {
            $connection->close();
        }
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
     */
    protected function addConnection(Connection $connection) {
        if (isset($this->connections[$connection])) {
            return;
        }

        $this->connections->attach($connection);
        $this->idle->push($connection);

        if ($this->deferred instanceof Deferred) {
            $this->deferred->resolve($connection);
        }
    }

    /**
     * @coroutine
     *
     * @return \Generator
     *
     * @resolve \Amp\Postgres\Connection
     */
    private function pop(): \Generator {
        while ($this->promise !== null) {
            try {
                yield $this->promise; // Prevent simultaneous connection creation.
            } catch (\Throwable $exception) {
                // Ignore failure or cancellation of other operations.
            }
        }

        while ($this->idle->isEmpty()) { // While loop to ensure an idle connection is available after promises below are resolved.
            try {
                if ($this->connections->count() >= $this->getMaxConnections()) {
                    // All possible connections busy, so wait until one becomes available.
                    $this->deferred = new Deferred;
                    yield $this->promise = $this->deferred->promise(); // May be resolved with defunct connection.
                } else {
                    // Max connection count has not been reached, so open another connection.
                    $this->promise = $this->createConnection();
                    $this->addConnection(yield $this->promise);
                }
            } finally {
                $this->deferred = null;
                $this->promise = null;
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
        return new Coroutine($this->doQuery($sql));
    }

    private function doQuery(string $sql): \Generator {
        /** @var \Amp\Mysql\Connection $connection */
        $connection = yield from $this->pop();

        try {
            $result = yield $connection->query($sql);
        } catch (\Throwable $exception) {
            $this->push($connection);
            throw $exception;
        }

        if ($result instanceof Operation) {
            $result->onComplete(function () use ($connection) {
                $this->push($connection);
            });
        } else {
            $this->push($connection);
        }

        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function prepare(string $sql): Promise {
        return new Coroutine($this->doPrepare($sql));
    }

    private function doPrepare(string $sql): \Generator {
        /** @var \Amp\Mysql\Connection $connection */
        $connection = yield from $this->pop();

        try {
            /** @var \Amp\Mysql\Statement $statement */
            $statement = yield $connection->prepare($sql);
        } catch (\Throwable $exception) {
            $this->push($connection);
            throw $exception;
        }

        $statement->onComplete(function () use ($connection) {
            $this->push($connection);
        });

        return $statement;
    }

    /**
     * {@inheritdoc}
     */
    public function execute(string $sql, array $params = []): Promise {
        return new Coroutine($this->doExecute($sql, $params));
    }

    private function doExecute(string $sql, array $params): \Generator {
        /** @var \Amp\Mysql\Connection $connection */
        $connection = yield from $this->pop();

        try {
            $result = yield $connection->execute($sql, $params);
        } catch (\Throwable $exception) {
            $this->push($connection);
            throw $exception;
        }

        if ($result instanceof Operation) {
            $result->onComplete(function () use ($connection) {
                $this->push($connection);
            });
        } else {
            $this->push($connection);
        }

        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function transaction(int $isolation = Transaction::COMMITTED): Promise {
        return new Coroutine($this->doTransaction($isolation));
    }

    private function doTransaction(int $isolation = Transaction::COMMITTED): \Generator {
        /** @var \Amp\Mysql\Connection $connection */
        $connection = yield from $this->pop();

        try {
            /** @var \Amp\Mysql\Transaction $transaction */
            $transaction = yield $connection->transaction($isolation);
        } catch (\Throwable $exception) {
            $this->push($connection);
            throw $exception;
        }

        $transaction->onComplete(function () use ($connection) {
            $this->push($connection);
        });

        return $transaction;
    }
}
