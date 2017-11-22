<?php

namespace Amp\Mysql;

use Amp\Promise;
use Amp\Socket\ClientTlsContext;

/**
 * @param string $connectionString
 * @param \Amp\Socket\ClientTlsContext $sslOptions
 *
 * @return \Amp\Promise<\Amp\Mysql\Connection>
 *
 * @throws \Amp\Mysql\FailureException If connecting fails.
 */
function connect(string $connectionString, ClientTlsContext $sslOptions = null): Promise {
    $config = ConnectionConfig::parseConnectionString($connectionString, $sslOptions);
    return Connection::connect($config);
}

/**
 * @param string $connectionString
 * @param \Amp\Socket\ClientTlsContext $sslOptions
 * @param int $maxConnections
 *
 * @return \Amp\Mysql\Pool
 */
function pool(
    string $connectionString,
    ClientTlsContext $sslOptions = null,
    int $maxConnections = ConnectionPool::DEFAULT_MAX_CONNECTIONS
): Pool {
    $config = ConnectionConfig::parseConnectionString($connectionString, $sslOptions);
    return new ConnectionPool($config, $maxConnections);
}
