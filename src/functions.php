<?php

namespace Amp\Mysql;

use Amp\Sql\Common\RetrySqlConnector;
use Amp\Sql\SqlConnector;
use Amp\Sql\SqlException;
use Revolt\EventLoop;

/**
 * @param SqlConnector<MysqlConfig, MysqlConnection>|null $connector
 *
 * @return SqlConnector<MysqlConfig, MysqlConnection>
 */
function mysqlConnector(?SqlConnector $connector = null): SqlConnector
{
    static $map;
    $map ??= new \WeakMap();
    $driver = EventLoop::getDriver();

    if ($connector) {
        return $map[$driver] = $connector;
    }

    return $map[$driver] ??= new RetrySqlConnector(new SocketMysqlConnector());
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
