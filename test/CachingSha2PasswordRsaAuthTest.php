<?php declare(strict_types=1);

namespace Amp\Mysql\Test;

use Amp\Mysql\Internal\ConnectionProcessor;
use Amp\Mysql\MysqlConfig;
use Amp\Mysql\SocketMysqlConnector;
use Amp\PHPUnit\AsyncTestCase;
use Amp\Sql\SqlConnectionException;
use Revolt\EventLoop;

/**
 * A fake server running the caching_sha2_password full authentication over plain TCP, as a MySQL server does when
 * the account is not in its auth cache yet: fast auth fails, the client requests the server RSA public key, then
 * sends the NUL-terminated password XORed with the 20-byte scramble and encrypted with that key.
 * Runs without a MySQL server.
 */
class CachingSha2PasswordRsaAuthTest extends AsyncTestCase
{
    /** 20 bytes, sent in the greeting as 8 bytes + 12 bytes and a trailing NUL. */
    private const SCRAMBLE = 'abcdefghijklmnopqrst';

    private static ?\OpenSSLAsymmetricKey $privateKey = null;

    private static function compilePacket(string $payload, int $sequenceId): string
    {
        return \substr(\pack('V', \strlen($payload)), 0, 3) . \chr($sequenceId) . $payload;
    }

    private static function makeGreeting(): string
    {
        $capabilities = ConnectionProcessor::CLIENT_PROTOCOL_41
            | ConnectionProcessor::CLIENT_SECURE_CONNECTION
            | ConnectionProcessor::CLIENT_PLUGIN_AUTH;

        $payload = "\x0a"
            . "8.0.40\0"
            . \pack('V', 1)
            . \substr(self::SCRAMBLE, 0, 8)
            . "\0"
            . \pack('v', $capabilities & 0xffff)
            . "\x21"
            . \pack('v', 0x0002)
            . \pack('v', $capabilities >> 16)
            . \chr(21)
            . \str_repeat("\0", 10)
            . \substr(self::SCRAMBLE, 8) . "\0"
            . "caching_sha2_password\0";

        return self::compilePacket($payload, 0);
    }

    private static function privateKey(): \OpenSSLAsymmetricKey
    {
        return self::$privateKey ??= \openssl_pkey_new([
            'private_key_type' => \OPENSSL_KEYTYPE_RSA,
            'private_key_bits' => 2048,
        ]);
    }

    /**
     * Runs the full authentication exchange with the client, then answers with an ERR packet so the connect attempt
     * always ends.
     *
     * @return list<string> The payloads of the packets sent by the client
     */
    private static function connect(string $password): array
    {
        $server = \stream_socket_server('tcp://127.0.0.1:0');
        $name = \stream_socket_get_name($server, false);
        $port = (int) \substr($name, \strrpos($name, ':') + 1);

        $publicKeyPem = \openssl_pkey_get_details(self::privateKey())['key'];

        $payloads = [];
        $watchers = [];

        $watchers[] = EventLoop::onReadable($server, static function (string $id, $server) use (
            &$payloads,
            &$watchers,
            $publicKeyPem,
        ): void {
            EventLoop::cancel($id);
            $client = \stream_socket_accept($server, 0);
            \stream_set_blocking($client, false);
            \fwrite($client, self::makeGreeting());

            $buffer = '';
            $watchers[] = EventLoop::onReadable($client, static function (string $id, $client) use (
                &$payloads,
                &$buffer,
                $publicKeyPem,
            ): void {
                $chunk = \fread($client, 65536);
                if ($chunk === '' || $chunk === false) {
                    EventLoop::cancel($id);
                    \fclose($client);
                    return;
                }

                $buffer .= $chunk;

                while (\strlen($buffer) >= 4) {
                    $length = \unpack('V', \substr($buffer, 0, 3) . "\0")[1];
                    if (\strlen($buffer) < 4 + $length) {
                        return;
                    }

                    $payloads[] = \substr($buffer, 4, $length);
                    $buffer = \substr($buffer, 4 + $length);

                    \fwrite($client, match (\count($payloads)) {
                        // Handshake response: fast authentication failed, full authentication is required
                        1 => self::compilePacket("\x01\x04", 2),
                        // Public key request: send the RSA public key
                        2 => self::compilePacket("\x01" . $publicKeyPem, 4),
                        // Encrypted password: end the exchange
                        default => self::compilePacket("\xff" . \pack('v', 1045) . '#28000Access denied', 6),
                    });
                }
            });
        });

        try {
            (new SocketMysqlConnector)->connect(new MysqlConfig('127.0.0.1', $port, 'app', $password));
            self::fail('The fake server always ends the exchange with an error');
        } catch (SqlConnectionException $exception) {
            // Any other failure means the exchange did not reach the end
            self::assertStringContainsString('Access denied', $exception->getMessage());
        } finally {
            foreach ($watchers as $watcher) {
                EventLoop::cancel($watcher);
            }
            \fclose($server);
        }

        return $payloads;
    }

    public function providePasswordLengths(): iterable
    {
        yield 'shorter than the scramble' => [19];
        yield 'as long as the scramble' => [20];
        yield 'longer than the scramble' => [21];
        yield 'twice as long as the scramble' => [40];
        yield 'twice the NUL-terminated plugin data length' => [42];
        yield 'longer than three scrambles' => [64];
    }

    /**
     * @dataProvider providePasswordLengths
     */
    public function testSendsPasswordXoredWithScrambleAndEncrypted(int $passwordLength): void
    {
        $this->setTimeout(5);

        $password = \substr(\str_repeat('0123456789abcdefghijklmnopqrstuvwxyz', 2), 0, $passwordLength);

        $payloads = self::connect($password);

        self::assertCount(3, $payloads);
        self::assertSame("\x02", $payloads[1], 'The client should request the server public key');

        self::assertTrue(\openssl_private_decrypt($payloads[2], $decrypted, self::privateKey(), \OPENSSL_PKCS1_OAEP_PADDING));
        $xorKey = \str_repeat(self::SCRAMBLE, (int) \ceil(\strlen($decrypted) / \strlen(self::SCRAMBLE)));

        self::assertSame(\bin2hex("$password\0"), \bin2hex($decrypted ^ $xorKey));
    }
}
