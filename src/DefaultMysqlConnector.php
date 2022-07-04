<?php

namespace Amp\Mysql;

use Amp\Cancellation;
use Amp\Socket;
use Amp\Sql\SqlConfig;

final class DefaultMysqlConnector implements MysqlConnector
{
    public function __construct(private readonly ?Socket\SocketConnector $connector = null)
    {
    }

    public function connect(SqlConfig $config, ?Cancellation $cancellation = null): MysqlConnection
    {
        if (!$config instanceof MysqlConfig) {
            throw new \TypeError(\sprintf("Must provide an instance of %s to MySQL connectors", MysqlConfig::class));
        }

        $socket = ($this->connector ?? Socket\socketConnector())
            ->connect($config->getConnectionString(), $config->getConnectContext(), $cancellation);

        return MysqlConnection::initialize($socket, $config, $cancellation);
    }
}
