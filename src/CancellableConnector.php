<?php

namespace Amp\Mysql;

use Amp\Cancellation;
use Amp\Socket;
use Amp\Sql\ConnectionConfig as SqlConnectionConfig;

final class CancellableConnector implements Connector
{
    public function __construct(private readonly ?Socket\SocketConnector $connector = null)
    {
    }

    public function connect(SqlConnectionConfig $config, ?Cancellation $token = null): Connection
    {
        if (!$config instanceof ConnectionConfig) {
            throw new \TypeError(\sprintf("Must provide an instance of %s to MySQL connectors", ConnectionConfig::class));
        }

        return Connection::connect($config, $token, $this->connector ?? Socket\socketConnector());
    }
}
