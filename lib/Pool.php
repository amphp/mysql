<?php

namespace Amp\Mysql;

use Amp\Promise;

interface Pool extends Link {
    /**
     * Extracts an idle connection from the pool. The connection is completely removed from the pool and cannot be
     * put back into the pool. Useful for operations where connection state must be changed.
     *
     * @return \Amp\Promise<\Amp\Mysql\Connection>
     */
    public function extractConnection(): Promise;

    /**
     * @return int Current number of connections in the pool.
     */
    public function getConnectionCount(): int;

    /**
     * @return int Current number of idle connections in the pool.
     */
    public function getIdleConnectionCount(): int;

    /**
     * @return int Maximum number of connections.
     */
    public function getMaxConnections(): int;
}
