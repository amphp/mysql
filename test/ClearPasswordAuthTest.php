<?php declare(strict_types=1);

namespace Amp\Mysql\Test;

use Amp\Mysql\Internal\ConnectionProcessor;
use Amp\Mysql\MysqlConfig;
use Amp\Mysql\SocketMysqlConnector;
use Amp\PHPUnit\AsyncTestCase;
use Amp\Sql\SqlConnectionException;
use Revolt\EventLoop;

/**
 * A fake server that asks for the mysql_clear_password auth plugin, as a rogue server or a
 * man in the middle would to harvest the password. Runs without a MySQL server.
 */
class ClearPasswordAuthTest extends AsyncTestCase
{
    private const PASSWORD = 'hunter2-secret';

    private static function compilePacket(string $payload, int $sequenceId): string
    {
        return \substr(\pack('V', \strlen($payload)), 0, 3) . \chr($sequenceId) . $payload;
    }

    private static function makeGreeting(string $authPlugin): string
    {
        $capabilities = ConnectionProcessor::CLIENT_PROTOCOL_41
            | ConnectionProcessor::CLIENT_SECURE_CONNECTION
            | ConnectionProcessor::CLIENT_PLUGIN_AUTH;

        $payload = "\x0a"
            . "8.0.40\0"
            . \pack('V', 1)
            . 'abcdefgh'
            . "\0"
            . \pack('v', $capabilities & 0xffff)
            . "\x21"
            . \pack('v', 0x0002)
            . \pack('v', $capabilities >> 16)
            . \chr(21)
            . \str_repeat("\0", 10)
            . "ijklmnopqrst\0"
            . "{$authPlugin}\0";

        return self::compilePacket($payload, 0);
    }

    /**
     * Serves $greeting, optionally answers the handshake response with an AuthSwitchRequest to
     * mysql_clear_password, records everything the client sends and finally answers with an ERR
     * packet so the connect attempt always ends.
     *
     * @param resource $server
     *
     * @return array{\Throwable|null, string}
     */
    private static function connect($server, MysqlConfig $config, string $greeting, bool $authSwitch): array
    {
        $received = '';
        $watchers = [];

        $watchers[] = EventLoop::onReadable($server, static function (string $id, $server) use (
            &$received,
            &$watchers,
            $greeting,
            $authSwitch,
        ): void {
            EventLoop::cancel($id);
            $client = \stream_socket_accept($server, 0);
            \stream_set_blocking($client, false);
            \fwrite($client, $greeting);

            $reads = 0;
            $watchers[] = EventLoop::onReadable($client, static function (string $id, $client) use (
                &$received,
                &$reads,
                $authSwitch,
            ): void {
                $chunk = \fread($client, 65536);
                if ($chunk === '' || $chunk === false) {
                    EventLoop::cancel($id);
                    \fclose($client);
                    return;
                }

                $received .= $chunk;

                if ($authSwitch && ++$reads === 1) {
                    \fwrite($client, self::compilePacket("\xfe" . "mysql_clear_password\0", 2));
                    return;
                }

                \fwrite($client, self::compilePacket("\xff" . \pack('v', 1045) . '#28000Access denied', $authSwitch ? 4 : 2));
            });
        });

        $error = null;

        try {
            (new SocketMysqlConnector)->connect($config);
        } catch (\Throwable $e) {
            $error = $e;
        } finally {
            foreach ($watchers as $watcher) {
                EventLoop::cancel($watcher);
            }
            \fclose($server);
        }

        return [$error, $received];
    }

    private static function messages(?\Throwable $error): string
    {
        $messages = '';
        for ($e = $error; $e !== null; $e = $e->getPrevious()) {
            $messages .= $e->getMessage() . "\n";
        }

        return $messages;
    }

    private static function tcpServer(): array
    {
        $server = \stream_socket_server('tcp://127.0.0.1:0');
        $name = \stream_socket_get_name($server, false);
        $port = (int) \substr($name, \strrpos($name, ':') + 1);

        return [$server, new MysqlConfig('127.0.0.1', $port, 'app', self::PASSWORD)];
    }

    public function testRefusesClearPasswordFromGreetingOverPlainTcp(): void
    {
        [$server, $config] = self::tcpServer();

        [$error, $received] = self::connect($server, $config, self::makeGreeting('mysql_clear_password'), false);

        self::assertInstanceOf(SqlConnectionException::class, $error);
        self::assertStringContainsString('mysql_clear_password', self::messages($error));
        self::assertStringNotContainsString(self::PASSWORD, $received);
    }

    public function testRefusesClearPasswordAuthSwitchOverPlainTcp(): void
    {
        [$server, $config] = self::tcpServer();

        [$error, $received] = self::connect($server, $config, self::makeGreeting('mysql_native_password'), true);

        self::assertInstanceOf(SqlConnectionException::class, $error);
        self::assertStringContainsString('mysql_clear_password', self::messages($error));
        self::assertStringNotContainsString(self::PASSWORD, $received);
    }

    public function testSendsClearPasswordOverUnixSocket(): void
    {
        $path = \sys_get_temp_dir() . '/amphp-mysql-clear-' . \bin2hex(\random_bytes(4)) . '.sock';
        $server = \stream_socket_server('unix://' . $path);

        try {
            [, $received] = self::connect(
                $server,
                new MysqlConfig($path, user: 'app', password: self::PASSWORD),
                self::makeGreeting('mysql_clear_password'),
                false,
            );
        } finally {
            @\unlink($path);
        }

        self::assertStringContainsString(self::PASSWORD, $received);
    }
}
