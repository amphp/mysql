<?php declare(strict_types=1);

namespace Amp\Mysql\Test;

use Amp\Cancellation;
use Amp\Mysql\MysqlConfig;
use Amp\Mysql\SocketMysqlConnection;
use Amp\PHPUnit\AsyncTestCase;
use Amp\Socket\ConnectContext;
use Amp\Socket\ConnectException;
use Amp\Socket\Socket;
use Amp\Socket\SocketAddress;
use Amp\Socket\SocketConnector;
use Amp\Sql\SqlException;

class SocketMysqlConnectionTest extends AsyncTestCase
{
    public function testConnectEnablesTcpNoDelay(): void
    {
        $connector = new class implements SocketConnector {
            public ?ConnectContext $context = null;

            public function connect(
                SocketAddress|string $uri,
                ?ConnectContext $context = null,
                ?Cancellation $cancellation = null,
            ): Socket {
                $this->context = $context;

                throw new ConnectException('Connection refused');
            }
        };

        $config = MysqlConfig::fromString('host=localhost', (new ConnectContext)->withConnectTimeout(5));

        try {
            SocketMysqlConnection::connect($connector, $config);
            self::fail(SqlException::class . ' was not thrown');
        } catch (SqlException) {
        }

        self::assertTrue($connector->context?->hasTcpNoDelay());
        self::assertSame(5.0, $connector->context->getConnectTimeout());
    }
}
