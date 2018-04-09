<?php

namespace Amp\Mysql;

use Amp\Loop;
use Amp\Promise;
use Amp\Socket\ClientTlsContext;

const LOOP_CONNECTOR_IDENTIFIER = Connector::class;

function connector(Connector $connector = null): Connector {
    if ($connector === null) {
        $connector = Loop::getState(LOOP_CONNECTOR_IDENTIFIER);
        if ($connector) {
            return $connector;
        }

        $connector = new TimeoutConnector;
    }

    Loop::setState(LOOP_CONNECTOR_IDENTIFIER, $connector);
    return $connector;
}

/**
 * Create a connection using the global Connector instance.
 *
 * @param string $connectionString
 * @param \Amp\Socket\ClientTlsContext $sslOptions
 *
 * @return \Amp\Promise<\Amp\Mysql\Connection>
 *
 * @throws \Amp\Mysql\FailureException If connecting fails.
 * @throws \Error If the connection string does not contain a host, user, and password.
 */
function connect(string $connectionString, ClientTlsContext $sslOptions = null): Promise {
    $config = ConnectionConfig::parseConnectionString($connectionString, $sslOptions);
    return connector()->connect($config);
}

/**
 * Create a pool using the global Connector instance.
 *
 * @param string $connectionString
 * @param \Amp\Socket\ClientTlsContext $sslOptions
 * @param int $maxConnections
 *
 * @return \Amp\Mysql\Pool
 *
 * @throws \Error If the connection string does not contain a host, user, and password.
 */
function pool(
    string $connectionString,
    ClientTlsContext $sslOptions = null,
    int $maxConnections = Pool::DEFAULT_MAX_CONNECTIONS
): Pool {
    $config = ConnectionConfig::parseConnectionString($connectionString, $sslOptions);
    return new Pool($config, $maxConnections, connector());
}
