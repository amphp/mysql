<?php

namespace Amp\Mysql;

use Amp\Deferred;
use Amp\Sql\AbstractPool;
use Amp\Sql\Connection;
use Amp\Sql\Connector;
use Amp\Sql\Pool as SqlPool;
use Amp\Sql\Statement;

final class Pool extends AbstractPool
{
    public function close()
    {
        $this->closed = true;
        while (!$this->idle->isEmpty()) {
            $connection = $this->idle->shift();
            $connection->close();
            $this->connections->detach($connection);
        }
    }

    /**
     * @return \Generator
     *
     * @resolve Connection
     *
     * @throws \Amp\Sql\FailureException If creating a connection fails.
     * @throws \Error If the pool has been closed.
     */
    protected function pop(): \Generator
    {
        if ($this->closed) {
            throw new \Error("The pool has been closed");
        }

        while ($this->promise !== null && $this->connections->count() + $this->pending >= $this->getMaxConnections()) {
            yield $this->promise; // Prevent simultaneous connection creation when connection count is at maximum - 1.
        }

        while (!$this->idle->isEmpty()) {
            $connection = $this->idle->shift();
            if ($connection->isAlive()) {
                return $connection;
            }

            $this->connections->detach($connection);
        }

        do { // While loop to ensure an idle connection is available after promises below are resolved.
            if ($this->connections->count() + $this->pending < $this->getMaxConnections()) {
                // Max connection count has not been reached, so open another connection.
                ++$this->pending;
                try {
                    $connection = yield $this->connector->connect($this->config);
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

            // All possible connections busy, so wait until one becomes available.
            try {
                $this->deferred = new Deferred;
                // May be resolved with defunct connection, but that connection will not be added to $this->idle.
                yield $this->promise = $this->deferred->promise();
            } finally {
                $this->deferred = null;
                $this->promise = null;
            }
        } while ($this->idle->isEmpty());

        $this->lastUsedAt = \time();

        // Shift a connection off the idle queue.
        return $this->idle->shift();
    }

    /**
     * @param Connection $connection
     *
     * @throws \Error If the connection is not part of this pool.
     */
    protected function push(Connection $connection)
    {
        \assert($this->connections->contains($connection), 'Connection is not part of this pool');

        if ($this->closed) {
            $connection->close();
            $this->connections->detach($connection);
            return;
        }

        if ($connection->isAlive()) {
            $this->idle->push($connection);
        } else {
            $this->connections->detach($connection);
        }

        if ($this->deferred instanceof Deferred) {
            $this->deferred->resolve($connection);
        }

        $this->lastUsedAt = \time();
    }

    protected function defaultConnector(): Connector
    {
        return connector();
    }

    protected function newPooledStatement(SqlPool $pool, Statement $statement, callable $prepare): Statement
    {
        return new Internal\PooledStatement($pool, $statement, $prepare);
    }
}
