<?php declare(strict_types=1);

namespace Amp\Mysql;

use Amp\Cancellation;
use Amp\Socket;
use Amp\Sql\SqlConfig;
use Amp\Sql\SqlConnector;

/**
 * @implements SqlConnector<MysqlConfig, MysqlConnection>
 */
final class SocketMysqlConnector implements SqlConnector
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

        return SocketMysqlConnection::initialize($socket, $config, $cancellation);
    }
}
