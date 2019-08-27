<?php

namespace Amp\Mysql;

use Amp\Loop;
use Amp\Promise;
use Amp\Sql\Common\ConnectionPool;
use Amp\Sql\ConnectionConfig as SqlConnectionConfig;
use Amp\Sql\Connector;

const LOOP_CONNECTOR_IDENTIFIER = Connector::class . "\\Mysql";

function connector(?Connector $connector = null): Connector
{
    if ($connector === null) {
        $connector = Loop::getState(LOOP_CONNECTOR_IDENTIFIER);
        if ($connector) {
            return $connector;
        }

        $connector = new CancellableConnector;
    }

    Loop::setState(LOOP_CONNECTOR_IDENTIFIER, $connector);
    return $connector;
}

/**
 * Create a connection using the global Connector instance.
 *
 * @param SqlConnectionConfig $config
 *
 * @return Promise<Connection>
 *
 * @throws \Amp\Sql\FailureException If connecting fails.
 * @throws \Error If the connection string does not contain a host, user, and password.
 */
function connect(SqlConnectionConfig $config): Promise
{
    return connector()->connect($config);
}

/**
 * Create a pool using the global Connector instance.
 *
 * @param SqlConnectionConfig $config
 * @param int $maxConnections
 * @param int $idleTimeout
 *
 * @return Pool
 *
 * @throws \Error If the connection string does not contain a host, user, and password.
 */
function pool(
    SqlConnectionConfig $config,
    int $maxConnections = ConnectionPool::DEFAULT_MAX_CONNECTIONS,
    int $idleTimeout = ConnectionPool::DEFAULT_IDLE_TIMEOUT
): Pool {
    return new Pool($config, $maxConnections, $idleTimeout, connector());
}
