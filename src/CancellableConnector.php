<?php

namespace Amp\Mysql;

use Amp\Cancellation;
use Amp\Socket;
use Amp\Sql\ConnectionConfig as SqlConnectionConfig;
use Amp\Sql\Connector;

final class CancellableConnector implements Connector
{
    private Socket\Connector $connector;

    public function __construct(?Socket\Connector $connector = null)
    {
        $this->connector = $connector ?? Socket\connector();
    }

    public function connect(SqlConnectionConfig $config, ?Cancellation $token = null): Connection
    {
        if (!$config instanceof ConnectionConfig) {
            throw new \TypeError(\sprintf("Must provide an instance of %s to MySQL connectors", ConnectionConfig::class));
        }

        return Connection::connect($config, $token, $this->connector);
    }
}
