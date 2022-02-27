<?php

namespace Amp\Mysql;

use Amp\Sql\Common\ConnectionPool;
use Amp\Sql\ConnectionConfig as SqlConnectionConfig;
use Amp\Sql\FailureException;
use Revolt\EventLoop;

function connector(?Connector $connector = null): Connector
{
    static $map;
    $map ??= new \WeakMap();
    $driver = EventLoop::getDriver();

    if ($connector) {
        return $map[$driver] = $connector;
    }

    return $map[$driver] ??= new CancellableConnector;
}

/**
 * Create a connection using the global Connector instance.
 *
 * @throws FailureException If connecting fails.
 * @throws \Error If the connection string does not contain a host, user, and password.
 */
function connect(SqlConnectionConfig $config): Connection
{
    return connector()->connect($config);
}

/**
 * Create a pool using the global Connector instance.
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
