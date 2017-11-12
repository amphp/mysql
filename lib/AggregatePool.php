<?php

namespace Amp\Mysql;

use Amp\Promise;

class AggregatePool extends AbstractPool {
    /**
     * @param \Amp\Mysql\Connection $connection
     */
    public function addConnection(Connection $connection) {
        parent::addConnection($connection);
    }

    /**
     * {@inheritdoc}
     */
    protected function createConnection(): Promise {
        throw new PoolError("Creating connections is not available in an aggregate pool");
    }

    /**
     * {@inheritdoc}
     */
    public function getMaxConnections(): int {
        $count = $this->getConnectionCount();

        if (!$count) {
            throw new PoolError("No connections in aggregate pool");
        }

        return $count;
    }
}
