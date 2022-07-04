<?php

namespace Amp\Mysql;

use Amp\Sql\SqlException;
use Revolt\EventLoop;

function mysqlConnector(?MysqlConnector $connector = null): MysqlConnector
{
    static $map;
    $map ??= new \WeakMap();
    $driver = EventLoop::getDriver();

    if ($connector) {
        return $map[$driver] = $connector;
    }

    return $map[$driver] ??= new DefaultMysqlConnector;
}

/**
 * Create a connection using the global Connector instance.
 *
 * @throws SqlException If connecting fails.
 * @throws \Error If the connection string does not contain a host, user, and password.
 */
function connect(MysqlConfig $config): MysqlConnection
{
    return mysqlConnector()->connect($config);
}
