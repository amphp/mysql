<?php

namespace Amp\Mysql;

use Amp\CancellationToken;
use Amp\Promise;
use Amp\Sql\ConnectionConfig as SqlConnectionConfig;
use Amp\Sql\Connector;

final class CancellableConnector implements Connector
{
    public function connect(SqlConnectionConfig $config, ?CancellationToken $token = null): Promise
    {
        if (!$config instanceof ConnectionConfig) {
            throw new \TypeError(\sprintf("Must provide an instance of %s to MySQL connectors", ConnectionConfig::class));
        }

        return Connection::connect($config, $token);
    }
}
