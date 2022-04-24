<?php

namespace Amp\Mysql\Internal;

use Amp\Cancellation;
use Amp\DeferredFuture;
use Amp\File;
use Amp\Future;
use Amp\Mysql\ColumnDefinition;
use Amp\Mysql\DataType;
use Amp\Mysql\InitializationException;
use Amp\Mysql\MysqlConfig;
use Amp\Socket\EncryptableSocket;
use Amp\Sql\ConnectionException;
use Amp\Sql\QueryError;
use Amp\Sql\SqlException;
use Amp\Sql\TransientResource;
use Revolt\EventLoop;

/* @TODO
 * 14.2.3 Auth switch request??
 * 14.2.4 COM_CHANGE_USER
 */

/** @internal */
class Processor implements TransientResource
{
    const STATEMENT_PARAM_REGEX = <<<'REGEX'
~(["'`])(?:\\(?:\\|\1)|(?!\1).)*+\1(*SKIP)(*FAIL)|(\?)|:([a-zA-Z_][a-zA-Z0-9_]*)~ms
REGEX;

    /** @var \Generator[] */
    private array $processors = [];

    private int $seqId = -1;
    private int $compressionId = -1;

    private ?EncryptableSocket $socket;

    private ?string $query = null;
    public array $named = [];

    private ?\Closure $parseCallback = null;
    private ?\Closure $packetCallback = null;

    public MysqlConfig $config;

    /** @var DeferredFuture[] */
    private array $deferreds = [];

    /** @var callable[] */
    private array $onReady = [];

    private ?ResultProxy $result = null;

    private int $lastUsedAt;

    private int $connectionId = 0;
    private string $authPluginData = '';
    private int $capabilities = 0;
    private int $serverCapabilities = 0;
    private string $authPluginName = '';
    private ConnectionState $connInfo;
    private int $refcount = 1;

    private int $connectionState = self::UNCONNECTED;

    private const MAX_PACKET_SIZE = 0xffffff;
    private const MAX_UNCOMPRESSED_BUFLEN = 0xfffffb;

    private const CLIENT_LONG_FLAG = 0x00000004;
    private const CLIENT_CONNECT_WITH_DB = 0x00000008;
    private const CLIENT_COMPRESS = 0x00000020;
    private const CLIENT_LOCAL_INFILE = 0x00000080;
    private const CLIENT_PROTOCOL_41 = 0x00000200;
    private const CLIENT_SSL = 0x00000800;
    private const CLIENT_TRANSACTIONS = 0x00002000;
    private const CLIENT_SECURE_CONNECTION = 0x00008000;
    private const CLIENT_MULTI_STATEMENTS = 0x00010000;
    private const CLIENT_MULTI_RESULTS = 0x00020000;
    private const CLIENT_PS_MULTI_RESULTS = 0x00040000;
    private const CLIENT_PLUGIN_AUTH = 0x00080000;
    private const CLIENT_CONNECT_ATTRS = 0x00100000;
    private const CLIENT_SESSION_TRACK = 0x00800000;
    private const CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000;
    private const CLIENT_DEPRECATE_EOF = 0x01000000;

    private const OK_PACKET = 0x00;
    private const EXTRA_AUTH_PACKET = 0x01;
    private const LOCAL_INFILE_REQUEST = 0xfb;
    private const AUTH_SWITCH_PACKET = 0xfe;
    private const EOF_PACKET = 0xfe;
    private const ERR_PACKET = 0xff;

    private const UNCONNECTED = 0;
    private const ESTABLISHED = 1;
    private const READY = 2;
    private const CLOSING = 3;
    private const CLOSED = 4;

    public function __construct(EncryptableSocket $socket, MysqlConfig $config)
    {
        $this->socket = $socket;
        $this->connInfo = new ConnectionState;
        $this->config = $config;
        $this->lastUsedAt = \time();
    }

    public function isAlive(): bool
    {
        return $this->connectionState <= self::READY;
    }

    public function isReady(): bool
    {
        return $this->connectionState === self::READY;
    }

    public function unreference(): void
    {
        if (!--$this->refcount) {
            $this->appendTask(function () {
                $this->close();
            });
        }
    }

    private function ready(): void
    {
        if (!empty($this->deferreds)) {
            return;
        }

        if (!empty($this->onReady)) {
            \array_shift($this->onReady)();
            return;
        }

        $this->resetIds();

        if ($this->socket) {
            try {
                $this->socket->unreference();
            } catch (EventLoop\InvalidCallbackError) {
                // Undefined destruct order can cause unref of an invalid watcher if the loop is swapped.
                // Generally this will only happen during tests.
            }
        }
    }

    private function addDeferred(DeferredFuture $deferred): void
    {
        \assert($this->socket !== null, "The connection has been closed");
        $this->deferreds[] = $deferred;
        $this->socket->reference();
    }

    public function connect(?Cancellation $cancellation = null): void
    {
        \assert(!$this->processors, self::class."::connect() must not be called twice");

        $this->deferreds[] = $deferred = new DeferredFuture; // Will be resolved in sendHandshake().

        $this->processors = [$this->parseMysql()];

        $id = $cancellation?->subscribe(fn () => $this->close());

        EventLoop::queue(fn () => $this->read());

        $future = $deferred->getFuture();
        if ($id !== null) {
            $future = $future->finally(static fn () => $cancellation?->unsubscribe($id));
        }

        if ($this->config->getCharset() !== MysqlConfig::DEFAULT_CHARSET
            || $this->config->getCollation() !== MysqlConfig::DEFAULT_COLLATE
        ) {
            $future = $future->map(function () {
                $charset = $this->config->getCharset();
                $collate = $this->config->getCollation();

                $this->query("SET NAMES '$charset'" . ($collate === "" ? "" : " COLLATE '$collate'"));
            });
        }

        $future->await();
    }

    private function read(): void
    {
        try {
            while ($this->socket !== null && ($bytes = $this->socket->read()) !== null) {
                // @codeCoverageIgnoreStart
                \assert((function () use ($bytes) {
                    if (\defined("MYSQL_DEBUG")) {
                        \fwrite(STDERR, "in: ");
                        for ($i = 0; $i < \min(\strlen($bytes), 200); $i++) {
                            \fwrite(STDERR, \dechex(\ord($bytes[$i])) . " ");
                        }
                        $r = \range("\0", "\x1f");
                        unset($r[10], $r[9]);
                        \fwrite(STDERR, "len: " . \strlen($bytes) . "\n");
                        \fwrite(STDERR, \str_replace($r, ".", \substr($bytes, 0, 200)) . "\n");
                    }

                    return true;
                })());
                // @codeCoverageIgnoreEnd

                $this->lastUsedAt = \time();

                $this->processData($bytes);
                $bytes = null; // Free last data read.
            }
        } catch (\Throwable $exception) {
            // $exception used as previous exception below.
        } finally {
            $this->close();
        }

        if (!empty($this->deferreds)) {
            $exception = new ConnectionException("Connection closed unexpectedly", 0, $exception ?? null);

            foreach ($this->deferreds as $deferred) {
                $deferred->error($exception);
            }

            $this->deferreds = [];
        }
    }

    private function processData(string $data): void
    {
        foreach ($this->processors as $processor) {
            if (empty($data = $processor->send($data))) {
                return;
            }
        }

        \assert(\is_array($data), "Final processor should yield an array");

        foreach ($data as $packet) {
            $this->parsePayload($packet);
        }
    }

    private function getDeferred(): DeferredFuture
    {
        return \array_shift($this->deferreds);
    }

    private function appendTask(\Closure $callback): void
    {
        if ($this->packetCallback
            || $this->parseCallback
            || !empty($this->onReady)
            || !empty($this->deferreds)
            || $this->connectionState !== self::READY
        ) {
            $this->onReady[] = $callback;
        } else {
            $callback();
        }
    }

    public function getConnInfo(): ConnectionState
    {
        return clone $this->connInfo;
    }

    public function getConnectionId(): int
    {
        return $this->connectionId;
    }

    public function getLastUsedAt(): int
    {
        return $this->lastUsedAt;
    }

    protected function startCommand(\Closure $callback): Future
    {
        if ($this->connectionState > self::READY) {
            throw new \Error("The connection has been closed");
        }

        $deferred = new DeferredFuture;
        $this->appendTask(function () use ($callback, $deferred) {
            $this->seqId = $this->compressionId = -1;
            $this->addDeferred($deferred);
            $callback();
        });
        return $deferred->getFuture();
    }

    public function setCharset(string $charset, string $collate = ""): Future
    {
        if ($collate === "" && false !== $offset = \strpos($charset, "_")) {
            $collate = $charset;
            $charset = \substr($collate, 0, $offset);
        }

        $query = "SET NAMES '$charset'" . ($collate === "" ? "" : " COLLATE '$collate'");
        $future = $this->query($query);

        $this->config = $this->config->withCharset($charset, $collate);

        return $future;
    }

    /** @see 14.6.3 COM_INIT_DB */
    public function useDb(string $db): Future
    {
        return $this->startCommand(function () use ($db): void {
            /** @psalm-suppress PropertyTypeCoercion */
            $this->config = $this->config->withDatabase($db);
            $this->sendPacket("\x02$db");
        });
    }

    /** @see 14.6.4 COM_QUERY */
    public function query(string $query): Future
    {
        return $this->startCommand(function () use ($query): void {
            $this->query = $query;
            $this->parseCallback = $this->handleQuery(...);
            $this->sendPacket("\x03$query");
        });
    }

    /** @see 14.7.4 COM_STMT_PREPARE */
    public function prepare(string $query): Future
    {
        return $this->startCommand(function () use ($query): void {
            $this->query = $query;
            $this->parseCallback = $this->handlePrepare(...);

            $query = \preg_replace_callback(self::STATEMENT_PARAM_REGEX, function ($m): string {
                static $index = 0;
                if ($m[2] !== "?") {
                    $this->named[$m[3]][] = $index;
                }
                $index++;
                return "?";
            }, $query);
            $this->sendPacket("\x16$query");
        });
    }

    /** @see 14.6.18 COM_CHANGE_USER */
    /* @TODO broken, my test server doesn't support that command, can't test now
    public function changeUser($user, $pass, $db = null) {
        return $this->startCommand(function() use ($user, $pass, $db) {
            $this->config->user = $user;
            $this->config->pass = $pass;
            $this->config->db = $db;
            $payload = "\x11";

            $payload .= "$user\0";
            $auth = $this->secureAuth($this->config->pass, $this->authPluginData);
            if ($this->capabilities & self::CLIENT_SECURE_CONNECTION) {
                $payload .= ord($auth) . $auth;
            } else {
                $payload .= "$auth\0";
            }
            $payload .= "$db\0";

            $this->sendPacket($payload);
            $this->parseCallback = [$this, "authSwitchRequest"];
        });
    }
    */

    /** @see 14.6.15 COM_PING */
    public function ping(): Future
    {
        return $this->startCommand(function (): void {
            $this->sendPacket("\x0e");
        });
    }

    /** @see 14.6.19 COM_RESET_CONNECTION */
    public function resetConnection(): Future
    {
        return $this->startCommand(function (): void {
            $this->sendPacket("\x1f");
        });
    }

    /** @see 14.7.5 COM_STMT_SEND_LONG_DATA */
    public function bindParam(int $stmtId, int $paramId, string $data): void
    {
        $payload = "\x18";
        $payload .= DataType::encodeInt32($stmtId);
        $payload .= DataType::encodeInt16($paramId);
        $payload .= $data;
        $this->appendTask(function () use ($payload) {
            $this->resetIds();
            $this->sendPacket($payload);
            $this->ready();
        });
    }

    /** @see 14.7.6 COM_STMT_EXECUTE
     * prebound params: null-bit set, type MYSQL_TYPE_LONG_BLOB, no value
     * $params is by-ref, because the actual result object might not yet have been filled completely with data upon
     * call of this method ...
     *
     * @param array<int, mixed> $prebound
     * @param array<int, mixed> $data
     */
    public function execute(int $stmtId, string $query, array &$params, array $prebound, array $data = []): Future
    {
        $deferred = new DeferredFuture;
        $this->appendTask(function () use ($stmtId, $query, &$params, $prebound, $data, $deferred): void {
            $payload = "\x17";
            $payload .= DataType::encodeInt32($stmtId);
            $payload .= \chr(0); // cursor flag // @TODO cursor types?!
            $payload .= DataType::encodeInt32(1);
            $paramCount = \count($params);
            $bound = (!empty($data) || !empty($prebound)) ? 1 : 0;
            $types = "";
            $values = "";
            if ($paramCount) {
                $args = $data + \array_fill(0, $paramCount, null);
                \ksort($args);
                $args = \array_slice($args, 0, $paramCount);
                $nullOff = \strlen($payload);
                $payload .= \str_repeat("\0", ($paramCount + 7) >> 3);
                foreach ($args as $paramId => $param) {
                    if ($param === null) {
                        $offset = $nullOff + ($paramId >> 3);
                        $payload[$offset] = $payload[$offset] | \chr(1 << ($paramId % 8));
                    } else {
                        $bound = 1;
                    }

                    /** @var DataType $type */
                    [$type, $value] = DataType::encodeBinary($param);
                    if (isset($prebound[$paramId])) {
                        $types .= \chr(DataType::LongBlob->value);
                    } else {
                        $types .= \chr($type->value);
                    }
                    $types .= "\0";
                    $values .= $value;
                }
                $payload .= \chr($bound);
                if ($bound) {
                    $payload .= $types;
                    $payload .= $values;
                }
            }

            $this->query = $query;

            $this->resetIds();
            $this->addDeferred($deferred);
            $this->sendPacket($payload);
            // apparently LOAD DATA LOCAL INFILE requests are not supported via prepared statements
            $this->packetCallback = $this->handleExecute(...);
        });
        return $deferred->getFuture(); // do not use $this->startCommand(), that might unexpectedly reset the seqId!
    }

    /** @see 14.7.7 COM_STMT_CLOSE */
    public function closeStmt(int $stmtId): void
    {
        $payload = "\x19" . DataType::encodeInt32($stmtId);
        $this->appendTask(function () use ($payload): void {
            if ($this->connectionState === self::READY) {
                $this->resetIds();
                $this->sendPacket($payload);
                $this->resetIds(); // does not expect a reply - must be reset immediately
            }
            $this->ready();
        });
    }

    /** @see 14.6.5 COM_FIELD_LIST */
    public function listFields(string $table, string $like = "%"): Future
    {
        return $this->startCommand(function () use ($table, $like): void {
            $this->sendPacket("\x04$table\0$like");
            $this->parseCallback = $this->handleFieldList(...);
        });
    }

    public function listAllFields(string $table, string $like = "%"): Future
    {
        $map = function (?array $array) use (&$map): array {
            static $columns = [];

            if ($array === null) {
                return $columns;
            }

            [$columns[], $future] = $array;
            return $future->map($map)->await();
        };

        return $this->listFields($table, $like)->map($map);
    }

    /** @see 14.6.6 COM_CREATE_DB */
    public function createDatabase(string $db): Future
    {
        return $this->startCommand(function () use ($db): void {
            $this->sendPacket("\x05$db");
        });
    }

    /** @see 14.6.7 COM_DROP_DB */
    public function dropDatabase(string $db): Future
    {
        return $this->startCommand(function () use ($db): void {
            $this->sendPacket("\x06$db");
        });
    }

    /**
     * @param int $subcommand
     * @see 14.6.8 COM_REFRESH
     */
    public function refresh(int $subcommand): Future
    {
        return $this->startCommand(function () use ($subcommand): void {
            $this->sendPacket("\x07" . \chr($subcommand));
        });
    }

    /** @see 14.6.9 COM_SHUTDOWN */
    public function shutdown(): Future
    {
        return $this->startCommand(function (): void {
            $this->sendPacket("\x08\x00"); /* SHUTDOWN_DEFAULT / SHUTDOWN_WAIT_ALL_BUFFERS, only one in use */
        });
    }

    /** @see 14.6.10 COM_STATISTICS */
    public function statistics(): Future
    {
        return $this->startCommand(function (): void {
            $this->sendPacket("\x09");
            $this->parseCallback = $this->readStatistics(...);
        });
    }

    /** @see 14.6.11 COM_PROCESS_INFO */
    public function processInfo(): Future
    {
        return $this->startCommand(function (): void {
            $this->sendPacket("\x0a");
            $this->query("SHOW PROCESSLIST");
        });
    }

    /** @see 14.6.13 COM_PROCESS_KILL */
    public function killProcess(int $process): Future
    {
        return $this->startCommand(function () use ($process): void {
            $this->sendPacket("\x0c" . DataType::encodeInt32($process));
        });
    }

    /** @see 14.6.14 COM_DEBUG */
    public function debugStdout(): Future
    {
        return $this->startCommand(function (): void {
            $this->sendPacket("\x0d");
        });
    }

    /** @see 14.7.8 COM_STMT_RESET */
    public function resetStmt(int $stmtId): Future
    {
        $payload = "\x1a" . DataType::encodeInt32($stmtId);
        $deferred = new DeferredFuture;
        $this->appendTask(function () use ($payload, $deferred): void {
            $this->resetIds();
            $this->addDeferred($deferred);
            $this->sendPacket($payload);
        });
        return $deferred->getFuture();
    }

    /** @see 14.8.4 COM_STMT_FETCH */
    public function fetchStmt(int $stmtId): Future
    {
        $payload = "\x1c" . DataType::encodeInt32($stmtId) . DataType::encodeInt32(1);
        $deferred = new DeferredFuture;
        $this->appendTask(function () use ($payload, $deferred): void {
            $this->resetIds();
            $this->addDeferred($deferred);
            $this->sendPacket($payload);
        });
        return $deferred->getFuture();
    }

    private function established(): void
    {
        // @TODO flags to use?
        $this->capabilities |= self::CLIENT_SESSION_TRACK
            | self::CLIENT_TRANSACTIONS
            | self::CLIENT_PROTOCOL_41
            | self::CLIENT_SECURE_CONNECTION
            | self::CLIENT_MULTI_RESULTS
            | self::CLIENT_PS_MULTI_RESULTS
            | self::CLIENT_MULTI_STATEMENTS
            | self::CLIENT_PLUGIN_AUTH
            | self::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA;

        if (\extension_loaded("zlib") && $this->config->isCompressionEnabled()) {
            $this->capabilities |= self::CLIENT_COMPRESS;
        }

        if ($this->config->isLocalInfileEnabled()) {
            $this->capabilities |=  self::CLIENT_LOCAL_INFILE;
        }
    }

    /** @see 14.1.3.2 ERR-Packet */
    private function handleError(string $packet): void
    {
        $offset = 1;

        $this->connInfo->errorCode = DataType::decodeUnsigned16($packet, $offset);

        if ($this->capabilities & self::CLIENT_PROTOCOL_41) {
            $this->connInfo->errorState = \substr($packet, $offset, 6);
            $offset += 6;
        }

        $this->connInfo->errorMsg = \substr($packet, $offset);

        $this->parseCallback = null;

        if ($this->connectionState < self::READY) {
            // connection failure
            $this->close();
            $this->getDeferred()->error(new InitializationException(\sprintf(
                'Could not connect to %s: %s %s',
                $this->config->getConnectionString(),
                $this->connInfo->errorState ?? 'Unknown state',
                $this->connInfo->errorMsg,
            )));
            return;
        }

        if ($this->result === null && empty($this->deferreds)) {
            // connection killed without pending query or active result
            $this->close();
            return;
        }

        $deferred = $this->result ?? $this->getDeferred();

        // normal error
        $exception = new QueryError(\sprintf(
            'MySQL error (%d): %s %s',
            $this->connInfo->errorCode,
            $this->connInfo->errorState ?? 'Unknown state',
            $this->connInfo->errorMsg,
        ), $this->query ?? '');

        $this->result = null;
        $this->query = null;

        $deferred->error($exception);

        $this->ready();
    }

    /**
     * @see 14.1.3.1 OK-Packet
     *
     * @psalm-suppress TypeDoesNotContainType Psalm seems to be having trouble with enums.
     */
    private function parseOk(string $packet): void
    {
        $offset = 1;

        $this->connInfo->affectedRows = DataType::decodeUnsigned($packet, $offset);
        $this->connInfo->insertId = DataType::decodeUnsigned($packet, $offset);

        if ($this->capabilities & (self::CLIENT_PROTOCOL_41 | self::CLIENT_TRANSACTIONS)) {
            $this->connInfo->statusFlags = DataType::decodeUnsigned16($packet, $offset);
            $this->connInfo->warnings = DataType::decodeUnsigned16($packet, $offset);
        }

        if (!($this->capabilities & self::CLIENT_SESSION_TRACK)) {
            $this->connInfo->statusInfo = \substr($packet, $offset);
            return;
        }

        // Even though it seems required according to 14.1.3.1, there is no length encoded string,
        // i.e. no trailing NULL byte ....???
        if (\strlen($packet) <= $offset) {
            $this->connInfo->statusInfo = "";
            return;
        }

        $this->connInfo->statusInfo = DataType::decodeString($packet, $offset);

        if (!StatusFlag::SessionStateChanged->inFlags($this->connInfo->statusFlags)) {
            return;
        }

        $sessionState = DataType::decodeString($packet, $offset);

        while (\strlen($packet) > $offset) {
            $data = DataType::decodeString($sessionState, $offset);
            $type = DataType::decodeUnsigned8($sessionState, $offset);

            switch (SessionStateType::tryFrom($type)) {
                case SessionStateType::SystemVariables:
                    $var = DataType::decodeString($data, $offset);
                    $this->connInfo->sessionState[$type][$var] = DataType::decodeString($data, $offset);
                    break;

                case SessionStateType::Schema:
                case SessionStateType::StateChange:
                case SessionStateType::Gtids:
                case SessionStateType::TransactionCharacteristics:
                case SessionStateType::TransactionState:
                    $this->connInfo->sessionState[$type] = DataType::decodeString($data, $offset);
                    break;

                default:
                    throw new \Error("$type is not a valid mysql session state type");
            }
        }
    }

    private function handleOk(string $packet): void
    {
        $this->parseOk($packet);
        $this->getDeferred()->complete(new CommandResult($this->connInfo->affectedRows, $this->connInfo->insertId));
        $this->ready();
    }

    /** @see 14.1.3.3 EOF-Packet */
    private function parseEof(string $packet): void
    {
        $offset = 1;
        if ($this->capabilities & self::CLIENT_PROTOCOL_41) {
            $this->connInfo->warnings = DataType::decodeUnsigned16($packet, $offset);
            $this->connInfo->statusFlags = DataType::decodeUnsigned16($packet, $offset);
        }
    }

    private function handleEof(string $packet): void
    {
        $this->parseEof($packet);
        $exception = new SqlException($this->connInfo->errorMsg ?? 'Unknown error', $this->connInfo->errorCode ?? 0);
        $this->getDeferred()->error($exception);
        $this->ready();
    }

    /** @see 14.2.5 Connection Phase Packets */
    private function handleHandshake(string $packet): void
    {
        $offset = 1;

        $protocol = \ord($packet);
        if ($protocol !== 0x0a) {
            throw new ConnectionException("Unsupported protocol version ".\ord($packet)." (Expected: 10)");
        }

        $this->connInfo->serverVersion = DataType::decodeNullTerminatedString($packet, $offset);

        $this->connectionId = DataType::decodeUnsigned32($packet, $offset);

        $this->authPluginData = \substr($packet, $offset, 8);
        $offset += 8;

        $offset += 1; // filler byte

        $this->serverCapabilities = DataType::decodeUnsigned16($packet, $offset);

        if (\strlen($packet) > $offset) {
            $this->connInfo->charset = DataType::decodeUnsigned8($packet, $offset);
            $this->connInfo->statusFlags = DataType::decodeUnsigned16($packet, $offset);
            $this->serverCapabilities += DataType::decodeUnsigned16($packet, $offset) << 16;

            $authPluginDataLen = $this->serverCapabilities & self::CLIENT_PLUGIN_AUTH
                ? DataType::decodeUnsigned8($packet, $offset)
                : 0;

            if ($this->serverCapabilities & self::CLIENT_SECURE_CONNECTION) {
                $offset += 10;

                $strlen = \max(13, $authPluginDataLen - 8);
                $this->authPluginData .= \substr($packet, $offset, $strlen);
                $offset += $strlen;

                if ($this->serverCapabilities & self::CLIENT_PLUGIN_AUTH) {
                    $this->authPluginName = DataType::decodeNullTerminatedString($packet, $offset);
                }
            }
        }

        $this->sendHandshake();
    }

    /** @see 14.2.5 Connection Phase Packets */
    private function handleAuthSwitch(string $packet): void
    {
        $offset = 1;
        $this->authPluginName = DataType::decodeNullTerminatedString($packet, $offset);
        $this->authPluginData = \substr($packet, $offset);
        $this->sendAuthSwitchResponse();
    }

    private function sendAuthSwitchResponse(): void
    {
        $this->write($this->getAuthData());
    }

    /** @see 14.6.4.1.2 LOCAL INFILE Request */
    private function handleLocalInfileRequest(string $packet): void
    {
        EventLoop::queue(function () use ($packet): void {
            try {
                $filePath = \substr($packet, 1);
                /** @var \Amp\File\File $fileHandle */
                if (!\function_exists("Amp\\File\\openFile")) {
                    throw new \Error("amphp/file must be installed for LOCAL INFILE queries");
                }

                $fileHandle = File\openFile($filePath, 'r');

                while (null !== ($chunk = $fileHandle->read())) {
                    $this->sendPacket($chunk);
                }
                $this->sendPacket("");
            } catch (\Throwable $e) {
                $this->getDeferred()->error(new ConnectionException("Failed to transfer a file to the server", 0, $e));
            }
        });
    }

    /** @see 14.6.4.1.1 Text Resultset */
    private function handleQuery(string $packet): void
    {
        switch (\ord($packet)) {
            case self::OK_PACKET:
                $this->parseOk($packet);

                if (StatusFlag::MoreResultsExist->inFlags($this->connInfo->statusFlags)) {
                    $result = new ResultProxy;
                    $result->affectedRows = $this->connInfo->affectedRows;
                    $result->insertId = $this->connInfo->insertId;
                    $this->getDeferred()->complete(new ConnectionResult($result));
                    $this->result = $result;
                    $result->updateState(ResultProxy::COLUMNS_FETCHED);
                    $this->successfulResultFetch();
                } else {
                    $this->parseCallback = null;
                    $this->getDeferred()->complete(new CommandResult(
                        $this->connInfo->affectedRows,
                        $this->connInfo->insertId
                    ));
                    $this->ready();
                }
                return;
            case self::LOCAL_INFILE_REQUEST:
                if ($this->config->isLocalInfileEnabled()) {
                    $this->handleLocalInfileRequest($packet);
                } else {
                    $this->getDeferred()->error(new ConnectionException("Unexpected LOCAL_INFILE_REQUEST packet"));
                }
                return;
            case self::ERR_PACKET:
                $this->handleError($packet);
                return;
        }

        $this->parseCallback = $this->handleTextColumnDefinition(...);
        $this->getDeferred()->complete(new ConnectionResult($result = new ResultProxy));
        /* we need to resolve before assigning vars, so that a onResolve() handler won't have a partial result available */
        $this->result = $result;
        $result->setColumns(DataType::decodeUnsigned($packet));
    }

    /** @see 14.7.1 Binary Protocol Resultset */
    private function handleExecute(string $packet): void
    {
        $this->parseCallback = $this->handleBinaryColumnDefinition(...);
        $this->getDeferred()->complete(new ConnectionResult($result = new ResultProxy));
        /* we need to resolve before assigning vars, so that a onResolve() handler won't have a partial result available */
        $this->result = $result;
        $result->setColumns(\ord($packet));
    }

    private function handleFieldList(string $packet): void
    {
        if (\ord($packet) === self::ERR_PACKET) {
            $this->parseCallback = null;
            $this->handleError($packet);
        } elseif (\ord($packet) === self::EOF_PACKET) {
            $this->parseCallback = null;
            $this->parseEof($packet);
            $this->getDeferred()->complete(null);
            $this->ready();
        } else {
            $this->addDeferred($deferred = new DeferredFuture);
            $this->getDeferred()->complete([$this->parseColumnDefinition($packet), $deferred]);
        }
    }

    private function handleTextColumnDefinition(string $packet): void
    {
        $this->handleColumnDefinition($packet, $this->handleTextResultSetRow(...));
    }

    private function handleBinaryColumnDefinition(string $packet): void
    {
        $this->handleColumnDefinition($packet, $this->handleBinaryResultSetRow(...));
    }

    private function handleColumnDefinition(string $packet, \Closure $parseCallback): void
    {
        \assert($this->result !== null, 'Connection result was in invalid state');

        if (!$this->result->columnsToFetch--) {
            $this->result->updateState(ResultProxy::COLUMNS_FETCHED);
            if (\ord($packet) === self::ERR_PACKET) {
                $this->parseCallback = null;
                $this->handleError($packet);
            } else {
                $cb = $this->parseCallback = $parseCallback;
                if ($this->capabilities & self::CLIENT_DEPRECATE_EOF) {
                    $cb($packet);
                } else {
                    $this->parseEof($packet);
                    // we don't need the EOF packet, skip!
                }
            }
            return;
        }

        $this->result->columns[] = $this->parseColumnDefinition($packet);
    }

    private function prepareParams(string $packet): void
    {
        \assert($this->result !== null, 'Connection result was in invalid state');

        if (!$this->result->columnsToFetch--) {
            $this->result->columnsToFetch = $this->result->columnCount;
            if (!$this->result->columnsToFetch) {
                $this->prepareFields($packet);
            } else {
                $this->parseCallback = $this->prepareFields(...);
            }
            return;
        }

        $this->result->params[] = $this->parseColumnDefinition($packet);
    }

    private function prepareFields(string $packet): void
    {
        \assert($this->result !== null, 'Connection result was in invalid state');

        if (!$this->result->columnsToFetch--) {
            $this->parseCallback = null;
            $this->query = null;
            $result = $this->result;
            $this->result = null;
            $this->ready();
            $result->updateState(ResultProxy::COLUMNS_FETCHED);

            return;
        }

        $this->result->columns[] = $this->parseColumnDefinition($packet);
    }

    /** @see 14.6.4.1.1.2 Column Defintion */
    private function parseColumnDefinition(string $packet): ColumnDefinition
    {
        $offset = 0;
        $column = [];

        if ($this->capabilities & self::CLIENT_PROTOCOL_41) {
            $column["catalog"] = DataType::decodeString($packet, $offset);
            $column["schema"] = DataType::decodeString($packet, $offset);
            $column["table"] = DataType::decodeString($packet, $offset);
            $column["originalTable"] = DataType::decodeString($packet, $offset);
            $column["name"] = DataType::decodeString($packet, $offset);
            $column["originalName"] = DataType::decodeString($packet, $offset);
            $fixLength = DataType::decodeUnsigned($packet, $offset);

            $column["charset"] = DataType::decodeUnsigned16($packet, $offset);
            $column["length"] = DataType::decodeUnsigned32($packet, $offset);
            $column["type"] = DataType::from(DataType::decodeUnsigned8($packet, $offset));
            $column["flags"] = DataType::decodeUnsigned16($packet, $offset);
            $column["decimals"] = DataType::decodeUnsigned8($packet, $offset);

            $offset += $fixLength;
        } else {
            $column["table"] = DataType::decodeString($packet, $offset);
            $column["name"] = DataType::decodeString($packet, $offset);

            $columnLength = DataType::decodeUnsigned($packet, $offset);
            $column["length"] = DataType::decodeIntByLength($packet, $columnLength, $offset);

            $typeLength = DataType::decodeUnsigned($packet, $offset);
            $column["type"] = DataType::from(DataType::decodeIntByLength($packet, $typeLength, $offset));

            $flagLength = $this->capabilities & self::CLIENT_LONG_FLAG
                ? DataType::decodeUnsigned($packet, $offset)
                : DataType::decodeUnsigned8($packet, $offset);

            if ($flagLength > 2) {
                $column["flags"] = DataType::decodeUnsigned16($packet, $offset);
            } else {
                $column["flags"] = DataType::decodeUnsigned8($packet, $offset);
            }

            $column["decimals"] = DataType::decodeUnsigned8($packet, $offset);
        }

        if ($offset < \strlen($packet)) {
            $column["defaults"] = DataType::decodeString($packet, $offset);
        }

        /** @psalm-suppress InvalidScalarArgument */
        return new ColumnDefinition(...$column);
    }

    private function successfulResultFetch(): void
    {
        \assert($this->result !== null, 'Connection result was in invalid state');

        $result = $this->result;
        $deferred = &$result->next;
        if (!$deferred) {
            $deferred = new DeferredFuture;
        }

        if (StatusFlag::MoreResultsExist->inFlags($this->connInfo->statusFlags)) {
            $this->parseCallback = $this->handleQuery(...);
            $this->addDeferred($deferred);
        } else {
            $this->parseCallback = null;
            $this->query = null;
            $this->result = null;
            $deferred->complete();
            $this->ready();
        }

        $result->updateState(ResultProxy::ROWS_FETCHED);
    }

    /** @see 14.6.4.1.1.3 Resultset Row */
    private function handleTextResultSetRow(string $packet): void
    {
        $packetType = \ord($packet);
        if ($packetType === self::EOF_PACKET) {
            if ($this->capabilities & self::CLIENT_DEPRECATE_EOF) {
                $this->parseOk($packet);
            } else {
                $this->parseEof($packet);
            }
            $this->successfulResultFetch();
            return;
        }

        if ($packetType === self::ERR_PACKET) {
            $this->handleError($packet);
            return;
        }

        \assert($this->result !== null, 'Connection result was in invalid state');

        $offset = 0;
        $fields = [];
        for ($i = 0; $offset < \strlen($packet); ++$i) {
            if (\ord($packet[$offset]) === 0xfb) {
                $fields[] = null;
                $offset += 1;
            } else {
                $column = $this->result->columns[$i] ?? throw new \RuntimeException("Definition missing for column $i");
                $fields[] = $column->type->decodeText($packet, $offset, $column->flags);
            }
        }

        $this->result->rowFetched($fields);
    }

    /** @see 14.7.2 Binary Protocol Resultset Row */
    private function handleBinaryResultSetRow(string $packet): void
    {
        $packetType = \ord($packet);
        if ($packetType === self::EOF_PACKET) {
            $this->parseEof($packet);
            $this->successfulResultFetch();
            return;
        } elseif ($packetType === self::ERR_PACKET) {
            $this->handleError($packet);
            return;
        }

        \assert($this->result !== null, 'Connection result was in invalid state');

        $offset = 1; // skip first byte
        $fields = [];
        for ($i = 0; $i < $this->result->columnCount; $i++) {
            if (\ord($packet[$offset + (($i + 2) >> 3)]) & (1 << (($i + 2) % 8))) {
                $fields[$i] = null;
            }
        }
        $offset += ($this->result->columnCount + 9) >> 3;

        for ($i = 0; $offset < \strlen($packet); $i++) {
            while (\array_key_exists($i, $fields)) {
                $i++;
            }

            $column = $this->result->columns[$i] ?? throw new \RuntimeException("Definition missing for column $i");
            $fields[$i] = $column->type->decodeBinary($packet, $offset, $column->flags);
        }

        $this->result->rowFetched($fields);
    }

    /** @see 14.7.4.1 COM_STMT_PREPARE Response */
    private function handlePrepare(string $packet): void
    {
        switch (\ord($packet)) {
            case self::OK_PACKET:
                break;
            case self::ERR_PACKET:
                $this->handleError($packet);
                return;
            default:
                throw new ConnectionException("Unexpected value for first byte of COM_STMT_PREPARE Response");
        }

        $offset = 1;

        $stmtId = DataType::decodeUnsigned32($packet, $offset);
        $columns = DataType::decodeUnsigned16($packet, $offset);
        $params = DataType::decodeUnsigned16($packet, $offset);

        $offset += 1; // filler

        $this->connInfo->warnings = DataType::decodeUnsigned16($packet, $offset);

        $this->result = new ResultProxy;
        $this->result->columnsToFetch = $params;
        $this->result->columnCount = $columns;
        $this->refcount++;
        \assert($this->query !== null, 'Invalid value for connection query');
        $this->getDeferred()->complete(new ConnectionStatement($this, $this->query, $stmtId, $this->named, $this->result));
        $this->named = [];
        if ($params) {
            $this->parseCallback = $this->prepareParams(...);
        } else {
            $this->prepareParams($packet);
        }
    }

    private function readStatistics(string $packet): void
    {
        $this->getDeferred()->complete($packet);
        $this->parseCallback = null;
        $this->ready();
    }

    /** @see 14.6.2 COM_QUIT */
    public function sendClose(): Future
    {
        return $this->startCommand(function () {
            $this->sendPacket("\x01");
            $this->connectionState = self::CLOSING;
        })->finally(fn () => $this->close());
    }

    public function close(): void
    {
        if ($this->connectionState === self::CLOSING && $this->deferreds) {
            \array_pop($this->deferreds)->complete();
        }

        $this->connectionState = self::CLOSED;

        if ($this->socket) {
            $this->socket->close();
            $this->socket = null;
        }

        $this->processors = [];
    }

    private function write(string $packet): void
    {
        $packet = $this->compilePacket($packet);

        // @codeCoverageIgnoreStart
        \assert((function () use ($packet) {
            if (\defined("MYSQL_DEBUG")) {
                \fwrite(STDERR, "out: ");
                for ($i = 0; $i < \min(\strlen($packet), 200); $i++) {
                    \fwrite(STDERR, \dechex(\ord($packet[$i])) . " ");
                }
                $r = \range("\0", "\x1f");
                unset($r[10], $r[9]);
                \fwrite(STDERR, "len: ".\strlen($packet)."\n");
                \fwrite(STDERR, \str_replace($r, ".", \substr($packet, 0, 200))."\n");
            }

            return true;
        })());
        // @codeCoverageIgnoreEnd

        if (($this->capabilities & self::CLIENT_COMPRESS) && $this->connectionState >= self::READY) {
            $packet = $this->compressPacket($packet);
        }

        \assert($this->socket !== null, 'The connection was closed during a call to write');
        $this->socket->write($packet);
    }

    private function resetIds(): void
    {
        $this->seqId = $this->compressionId = -1;
    }

    private function compilePacket(string $pending): string
    {
        $packet = "";
        do {
            $length = \strlen($pending);
            if ($length >= (1 << 24) - 1) {
                $out = \substr($pending, 0, (1 << 24) - 1);
                $pending = \substr($pending, (1 << 24) - 1);
                $length = (1 << 24) - 1;
            } else {
                $out = $pending;
                $pending = "";
            }
            $packet .= \substr_replace(\pack("V", $length), \chr(++$this->seqId), 3, 1) . $out; // expects $length < (1 << 24) - 1
        } while ($pending !== "");

        return $packet;
    }

    private function compressPacket(string $packet): string
    {
        if ($packet === "") {
            return "";
        }

        $length = \strlen($packet);
        $deflated = \zlib_encode($packet, ZLIB_ENCODING_DEFLATE);

        if ($length < \strlen($deflated)) {
            return \substr_replace(\pack("V", \strlen($packet)), \chr(++$this->compressionId), 3, 1) . "\0\0\0" . $packet;
        }

        return \substr_replace(\pack("V", \strlen($deflated)), \chr(++$this->compressionId), 3, 1)
            . \substr(\pack("V", $length), 0, 3) . $deflated;
    }

    /** @see 14.4 Compression */
    private function parseCompression(): \Generator
    {
        $inflated = "";
        $buffer = "";

        while (true) {
            while (\strlen($buffer) < 7) {
                $buffer .= yield $inflated;
                $inflated = "";
            }

            $size = DataType::decodeUnsigned24($buffer);
            $this->compressionId = \ord($buffer[3]);
            $uncompressed = DataType::decodeUnsigned24(\substr($buffer, 4, 3));

            $buffer = \substr($buffer, 7);

            if ($size > 0) {
                while (\strlen($buffer) < $size) {
                    $buffer .= yield $inflated;
                    $inflated = "";
                }

                if ($uncompressed === 0) {
                    $inflated .= \substr($buffer, 0, $size);
                } else {
                    $inflated .= \zlib_decode(\substr($buffer, 0, $size), $uncompressed);
                }

                $buffer = \substr($buffer, $size);
            }
        }
    }

    /**
     * @see 14.1.2 MySQL Packet
     * @see 14.1.3 Generic Response Packets
     */
    private function parseMysql(): \Generator
    {
        $buffer = "";
        $parsed = [];

        while (true) {
            $packet = "";

            do {
                while (\strlen($buffer) < 4) {
                    $buffer .= yield $parsed;
                    $parsed = [];
                }

                $length = DataType::decodeUnsigned24($buffer);
                $this->seqId = \ord($buffer[3]);
                $buffer = \substr($buffer, 4);

                while (\strlen($buffer) < ($length & 0xffffff)) {
                    $buffer .= yield $parsed;
                    $parsed = [];
                }

                $lastIn = $length !== 0xffffff;
                if ($lastIn) {
                    $size = $length % 0xffffff;
                } else {
                    $size = 0xffffff;
                }

                $packet .= \substr($buffer, 0, $size);
                $buffer = \substr($buffer, $size);
            } while (!$lastIn);

            if (\strlen($packet) > 0) {
                $parsed[] = $packet;
            }
        }
    }

    private function parsePayload(string $packet): void
    {
        if ($this->connectionState === self::UNCONNECTED) {
            $this->established();
            $this->connectionState = self::ESTABLISHED;
            $this->handleHandshake($packet);
            return;
        }

        if ($this->connectionState === self::ESTABLISHED) {
            switch (\ord($packet)) {
                case self::OK_PACKET:
                    if ($this->capabilities & self::CLIENT_COMPRESS) {
                        $this->processors = \array_merge([$this->parseCompression()], $this->processors);
                    }
                    $this->connectionState = self::READY;
                    $this->handleOk($packet);
                    break;
                case self::ERR_PACKET:
                    $this->handleError($packet);
                    break;
                case self::AUTH_SWITCH_PACKET:
                    $this->handleAuthSwitch($packet);
                    break;
                case self::EXTRA_AUTH_PACKET:
                    /** @see 14.2.5 Connection Phase Packets (AuthMoreData) */
                    switch ($this->authPluginName) {
                        case "sha256_password":
                            $key = \substr($packet, 1);
                            $this->config = $this->config->withKey($key);
                            $this->sendHandshake();
                            break;
                        case "caching_sha2_password":
                            switch (\ord(\substr($packet, 1, 1))) {
                                case 3: // success
                                    return; // expecting OK afterwards
                                case 4: // fast auth failure
                                    /* unix domain socket, information not trivially available from $this->socket */
                                    if ($this->capabilities & self::CLIENT_SSL || $this->config->getHost()[0] === "/") {
                                        $this->write($this->config->getPassword() . "\0");
                                    } else {
                                        $this->write("\x02");
                                    }
                                    break;
                                case 0x2d: // certificate
                                    $pubkey = \substr($packet, 1);
                                    $this->write($this->sha256Auth($this->config->getPassword() ?? '', $this->authPluginData, $pubkey));
                                    break;
                            }
                            break;
                        default:
                            throw new ConnectionException(
                                "Unexpected EXTRA_AUTH_PACKET in authentication phase for method {$this->authPluginName}"
                            );
                    }
                    break;
            }
            return;
        }

        if ($this->parseCallback) {
            ($this->parseCallback)($packet);
            return;
        }

        $cb = $this->packetCallback;
        $this->packetCallback = null;
        switch (\ord($packet)) {
            case self::OK_PACKET:
                $this->handleOk($packet);
                break;
            case self::ERR_PACKET:
                $this->handleError($packet);
                break;
            case self::EOF_PACKET:
                if (\strlen($packet) < 6) {
                    $this->handleEof($packet);
                    break;
                }
                // no break
            default:
                if (!$cb) {
                    throw new ConnectionException("Unexpected packet type: " . \ord($packet));
                }

                $cb($packet);
        }
    }

    private function secureAuth(string $pass, string $scramble): string
    {
        $hash = \sha1($pass, true);
        return $hash ^ \sha1(\substr($scramble, 0, 20) . \sha1($hash, true), true);
    }

    private function sha256Auth(string $pass, string $scramble, string $key): string
    {
        \openssl_public_encrypt(
            "$pass\0" ^ \str_repeat($scramble, (int) \ceil(\strlen($pass) / \strlen($scramble))),
            $auth,
            $key,
            OPENSSL_PKCS1_OAEP_PADDING,
        );

        return $auth;
    }

    private function sha2Auth(string $pass, string $scramble): string
    {
        $hash = \hash("sha256", $pass, true);
        return $hash ^ \hash("sha256", \substr($scramble, 0, 20) . \hash("sha256", $hash, true), true);
    }

    private function authSwitchRequest(string $packet): void
    {
        $this->parseCallback = null;
        switch (\ord($packet)) {
            case self::EOF_PACKET:
                if (\strlen($packet) === 1) {
                    break;
                }
                $length = (int) \strpos($packet, "\0");
                $pluginName = \substr($packet, 0, $length); // @TODO mysql_native_pass only now...
                $authPluginData = \substr($packet, $length + 1);
                $this->sendPacket($this->secureAuth($this->config->getPassword() ?? '', $authPluginData));
                break;
            case self::ERR_PACKET:
                $this->handleError($packet);
                return;
            default:
                throw new ConnectionException("AuthSwitchRequest: Expecting 0xfe (or ERR_Packet), got 0x".\dechex(\ord($packet)));
        }
    }

    /**
     * @see 14.2.5 Connection Phase Packets
     * @see 14.3 Authentication Method
     */
    private function sendHandshake(bool $inSSL = false): void
    {
        if ($this->config->getDatabase() !== null) {
            $this->capabilities |= self::CLIENT_CONNECT_WITH_DB;
        }

        if ($this->config->getConnectContext()->getTlsContext() !== null) {
            $this->capabilities |= self::CLIENT_SSL;
        }

        $this->capabilities &= $this->serverCapabilities;

        $payload = "";
        $payload .= \pack("V", $this->capabilities);
        $payload .= \pack("V", 1 << 24 - 1); // max-packet size
        $payload .= \chr(MysqlConfig::BIN_CHARSET);
        $payload .= \str_repeat("\0", 23); // reserved

        if (!$inSSL && ($this->capabilities & self::CLIENT_SSL)) {
            EventLoop::queue(function () use ($payload): void {
                try {
                    $this->write($payload);

                    $this->socket?->setupTls();

                    $this->sendHandshake(true);
                } catch (\Throwable $e) {
                    $this->close();
                    $this->getDeferred()->error($e);
                }
            });

            return;
        }

        $payload .= $this->config->getUser()."\0";

        $auth = $this->getAuthData();
        if ($this->capabilities & self::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) {
            $payload .= DataType::encodeInt(\strlen($auth));
            $payload .= $auth;
        } elseif ($this->capabilities & self::CLIENT_SECURE_CONNECTION) {
            $payload .= \chr(\strlen($auth));
            $payload .= $auth;
        } else {
            $payload .= "$auth\0";
        }
        if ($this->capabilities & self::CLIENT_CONNECT_WITH_DB) {
            $payload .= "{$this->config->getDatabase()}\0";
        }
        if ($this->capabilities & self::CLIENT_PLUGIN_AUTH) {
            $payload .= "{$this->authPluginName}\0";
        }
        if ($this->capabilities & self::CLIENT_CONNECT_ATTRS) {
            // connection attributes?! 5.6.6+ only!
        }
        $this->write($payload);
    }

    private function getAuthData(): string
    {
        $password = $this->config->getPassword() ?? "";

        if ($this->config->getPassword() == "") {
            return "";
        }

        if ($this->capabilities & self::CLIENT_PLUGIN_AUTH) {
            switch ($this->authPluginName) {
                case "mysql_native_password":
                    return $this->secureAuth($password, $this->authPluginData);
                case "mysql_clear_password":
                    return $password;
                case "sha256_password":
                    $key = $this->config->getKey();
                    if ($key !== '') {
                        return $this->sha256Auth($password, $this->authPluginData, $key);
                    }
                    return "\x01";
                case "caching_sha2_password":
                    return $this->sha2Auth($password, $this->authPluginData);
                case "mysql_old_password":
                    throw new ConnectionException(
                        "mysql_old_password is outdated and insecure. Intentionally not implemented!"
                    );
                default:
                    throw new ConnectionException(
                        "Invalid (or unimplemented?) auth method requested by server: {$this->authPluginName}"
                    );
            }
        }

        return $this->secureAuth($password, $this->authPluginData);
    }

    /** @see 14.1.2 MySQL Packet */
    protected function sendPacket(string $payload): void
    {
        if ($this->connectionState !== self::READY) {
            throw new \Error("Connection not ready, cannot send any packets");
        }

        $this->write($payload);
    }
}
