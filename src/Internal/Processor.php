<?php

namespace Amp\Mysql\Internal;

use Amp\Coroutine;
use Amp\Deferred;
use Amp\Loop;
use Amp\Mysql\CommandResult;
use Amp\Mysql\ConnectionConfig;
use Amp\Mysql\ConnectionStatement;
use Amp\Mysql\DataTypes;
use Amp\Mysql\InitializationException;
use Amp\Promise;
use Amp\Socket\ClientTlsContext;
use Amp\Socket\Socket;
use Amp\Sql\ConnectionException;
use Amp\Sql\FailureException;
use Amp\Sql\QueryError;
use Amp\Sql\TransientResource;

/* @TODO
 * 14.2.3 Auth switch request??
 * 14.2.4 COM_CHANGE_USER
 */

/** @see 14.1.3.4 Status Flags */
class StatusFlags
{
    const SERVER_STATUS_IN_TRANS = 0x0001; // a transaction is active
    const SERVER_STATUS_AUTOCOMMIT = 0x0002; // auto-commit is enabled
    const SERVER_MORE_RESULTS_EXISTS = 0x0008;
    const SERVER_STATUS_NO_GOOD_INDEX_USED = 0x0010;
    const SERVER_STATUS_NO_INDEX_USED = 0x0020;
    const SERVER_STATUS_CURSOR_EXISTS = 0x0040; // Used by Binary Protocol Resultset to signal that COM_STMT_FETCH has to be used to fetch the row-data.
    const SERVER_STATUS_LAST_ROW_SENT = 0x0080;
    const SERVER_STATUS_DB_DROPPED = 0x0100;
    const SERVER_STATUS_NO_BACKSLASH_ESCAPES = 0x0200;
    const SERVER_STATUS_METADATA_CHANGED = 0x0400;
    const SERVER_QUERY_WAS_SLOW = 0x0800;
    const SERVER_PS_OUT_PARAMS = 0x1000;
    const SERVER_STATUS_IN_TRANS_READONLY = 0x2000; // in a read-only transaction
    const SERVER_SESSION_STATE_CHANGED = 0x4000; // connection state information has changed
}

/** @see 13.1.3.1.1 Session State Information */
class SessionStateTypes
{
    const SESSION_TRACK_SYSTEM_VARIABLES = 0x00;
    const SESSION_TRACK_SCHEMA = 0x01;
    const SESSION_TRACK_STATE_CHANGE = 0x02;
}

/** @internal */
class Processor implements TransientResource
{
    const STATEMENT_PARAM_REGEX = <<<'REGEX'
~(["'`])(?:\\(?:\\|\1)|(?!\1).)*+\1(*SKIP)(*FAIL)|(\?)|:([a-zA-Z_][a-zA-Z0-9_]*)~ms
REGEX;

    /** @var \Generator[] */
    private $processors = [];

    /** @var int */
    private $protocol;

    private $seqId = -1;
    private $compressionId = -1;

    /** @var \Amp\Socket\ClientSocket */
    private $socket;

    private $authPluginDataLen;
    private $query;
    public $named = [];

    /** @var callable|null */
    private $parseCallback = null;
    /** @var callable|null */
    private $packetCallback = null;

    /** @var \Amp\Promise|null */
    private $pendingWrite;

    /** @var \Amp\Sql\ConnectionConfig */
    public $config;

    /** @var \Amp\Deferred[] */
    private $deferreds = [];

    /** @var callable[] */
    private $onReady = [];

    /** @var ResultProxy|null */
    private $result;

    /** @var int */
    private $lastUsedAt;

    private $connectionId;
    private $authPluginData;
    private $capabilities = 0;
    private $serverCapabilities = 0;
    private $authPluginName;
    private $connInfo;
    private $refcount = 1;

    private $connectionState = self::UNCONNECTED;

    const MAX_PACKET_SIZE = 0xffffff;
    const MAX_UNCOMPRESSED_BUFLEN = 0xfffffb;

    const CLIENT_LONG_FLAG = 0x00000004;
    const CLIENT_CONNECT_WITH_DB = 0x00000008;
    const CLIENT_COMPRESS = 0x00000020;
    const CLIENT_LOCAL_INFILE = 0x00000080;
    const CLIENT_PROTOCOL_41 = 0x00000200;
    const CLIENT_SSL = 0x00000800;
    const CLIENT_TRANSACTIONS = 0x00002000;
    const CLIENT_SECURE_CONNECTION = 0x00008000;
    const CLIENT_MULTI_STATEMENTS = 0x00010000;
    const CLIENT_MULTI_RESULTS = 0x00020000;
    const CLIENT_PS_MULTI_RESULTS = 0x00040000;
    const CLIENT_PLUGIN_AUTH = 0x00080000;
    const CLIENT_CONNECT_ATTRS = 0x00100000;
    const CLIENT_SESSION_TRACK = 0x00800000;
    const CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000;
    const CLIENT_DEPRECATE_EOF = 0x01000000;

    const OK_PACKET = 0x00;
    const EXTRA_AUTH_PACKET = 0x01;
    const LOCAL_INFILE_REQUEST = 0xfb;
    const EOF_PACKET = 0xfe;
    const ERR_PACKET = 0xff;

    const UNCONNECTED = 0;
    const ESTABLISHED = 1;
    const READY = 2;
    const CLOSING = 3;
    const CLOSED = 4;

    public function __construct(Socket $socket, ConnectionConfig $config)
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

    public function unreference()
    {
        if (!--$this->refcount) {
            $this->appendTask(function () {
                $this->close();
            });
        }
    }

    private function ready()
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
            } catch (Loop\InvalidWatcherError $exception) {
                // Undefined destruct order can cause unref of an invalid watcher if the loop is swapped.
                // Generally this will only happen during tests.
            }
        }
    }

    private function addDeferred(Deferred $deferred)
    {
        \assert($this->socket, "The connection has been closed");
        $this->deferreds[] = $deferred;
        $this->socket->reference();
    }

    public function connect(): Promise
    {
        \assert(!$this->processors, self::class."::connect() must not be called twice");

        $this->deferreds[] = $deferred = new Deferred; // Will be resolved below or in sendHandshake().

        $this->processors = [$this->parseMysql()];

        (new Coroutine($this->read()))->onResolve(function () {
            $this->close();

            foreach ($this->deferreds as $deferred) {
                if ($this->query === "") {
                    $deferred->fail(new InitializationException("Connection went away"));
                } else {
                    $deferred->fail(new ConnectionException("Connection went away... unable to fulfil this deferred ... It's unknown whether the query was executed..."));
                }
            }
        });

        $promise = $deferred->promise();

        if ($this->config->getCharset() !== ConnectionConfig::DEFAULT_CHARSET || $this->config->getCollation() !== ConnectionConfig::DEFAULT_COLLATE) {
            $promise->onResolve(function ($exception) {
                if ($exception) {
                    return;
                }

                $charset = $this->config->getCharset();
                $collate = $this->config->getCollation();

                $this->query("SET NAMES '$charset'" . ($collate === "" ? "" : " COLLATE '$collate'"));
            });
        }

        return $promise;
    }

    private function read(): \Generator
    {
        while (($bytes = yield $this->socket->read()) !== null) {
            // @codeCoverageIgnoreStart
            \assert((function () use ($bytes) {
                if (\defined("MYSQL_DEBUG")) {
                    \fwrite(STDERR, "in: ");
                    for ($i = 0; $i < \min(\strlen($bytes), 200); $i++) {
                        \fwrite(STDERR, \dechex(\ord($bytes[$i])) . " ");
                    }
                    $r = \range("\0", "\x1f");
                    unset($r[10], $r[9]);
                    \fwrite(STDERR, "len: ".\strlen($bytes)."\n");
                    \fwrite(STDERR, \str_replace($r, ".", \substr($bytes, 0, 200))."\n");
                }

                return true;
            })());
            // @codeCoverageIgnoreEnd

            $this->lastUsedAt = \time();

            $this->processData($bytes);
            $bytes = null; // Free last data read.

            if (!$this->socket) { // Connection closed.
                break;
            }
        }
    }

    private function processData(string $data)
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

    private function getDeferred(): Deferred
    {
        return \array_shift($this->deferreds);
    }

    private function appendTask(callable $callback)
    {
        if ($this->packetCallback || $this->parseCallback || !empty($this->onReady) || !empty($this->deferreds) || $this->connectionState !== self::READY) {
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

    protected function startCommand(callable $callback): Promise
    {
        if ($this->connectionState > self::READY) {
            throw new \Error("The connection has been closed");
        }

        $deferred = new Deferred;
        $this->appendTask(function () use ($callback, $deferred) {
            $this->seqId = $this->compressionId = -1;
            $this->addDeferred($deferred);
            $callback();
        });
        return $deferred->promise();
    }

    public function setCharset(string $charset, string $collate = ""): Promise
    {
        return \Amp\call(function () use ($charset, $collate) {
            if ($collate === "" && false !== $off = \strpos($charset, "_")) {
                $collate = $charset;
                $charset = \substr($collate, 0, $off);
            }

            $query = "SET NAMES '$charset'" . ($collate === "" ? "" : " COLLATE '$collate'");
            $result = yield $this->query($query);

            $this->config = $this->config->withCharset($charset, $collate);

            return $result;
        });
    }


    /** @see 14.6.3 COM_INIT_DB */
    public function useDb(string $db): Promise
    {
        return $this->startCommand(function () use ($db) {
            $this->config = $this->config->withDatabase($db);
            $this->sendPacket("\x02$db");
        });
    }

    /** @see 14.6.4 COM_QUERY */
    public function query(string $query): Promise
    {
        return $this->startCommand(function () use ($query) {
            $this->query = $query;
            $this->parseCallback = [$this, "handleQuery"];
            $this->sendPacket("\x03$query");
        });
    }

    /** @see 14.7.4 COM_STMT_PREPARE */
    public function prepare(string $query): Promise
    {
        return $this->startCommand(function () use ($query) {
            $this->query = $query;
            $this->parseCallback = [$this, "handlePrepare"];

            $query = \preg_replace_callback(self::STATEMENT_PARAM_REGEX, function ($m) {
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
    public function ping(): Promise
    {
        return $this->startCommand(function () {
            $this->sendPacket("\x0e");
        });
    }

    /** @see 14.6.19 COM_RESET_CONNECTION */
    public function resetConnection(): Promise
    {
        return $this->startCommand(function () {
            $this->sendPacket("\x1f");
        });
    }

    /** @see 14.7.5 COM_STMT_SEND_LONG_DATA */
    public function bindParam(int $stmtId, int $paramId, string $data)
    {
        $payload = "\x18";
        $payload .= DataTypes::encodeInt32($stmtId);
        $payload .= DataTypes::encodeInt16($paramId);
        $payload .= $data;
        $this->appendTask(function () use ($payload) {
            $this->resetIds();
            $this->sendPacket($payload);
            $this->ready();
        });
    }

    /** @see 14.7.6 COM_STMT_EXECUTE */
    // prebound params: null-bit set, type MYSQL_TYPE_LONG_BLOB, no value
    // $params is by-ref, because the actual result object might not yet have been filled completely with data upon call of this method ...
    public function execute(int $stmtId, string $query, array &$params, array $prebound, array $data = []): Promise
    {
        $deferred = new Deferred;
        $this->appendTask(function () use ($stmtId, $query, &$params, $prebound, $data, $deferred) {
            $payload = "\x17";
            $payload .= DataTypes::encodeInt32($stmtId);
            $payload .= \chr(0); // cursor flag // @TODO cursor types?!
            $payload .= DataTypes::encodeInt32(1);
            $paramCount = \count($params);
            $bound = !empty($data) || !empty($prebound);
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
                        $off = $nullOff + ($paramId >> 3);
                        $payload[$off] = $payload[$off] | \chr(1 << ($paramId % 8));
                    } else {
                        $bound = 1;
                    }
                    list($unsigned, $type, $value) = DataTypes::encodeBinary($param);
                    if (isset($prebound[$paramId])) {
                        $types .= \chr(DataTypes::MYSQL_TYPE_LONG_BLOB);
                    } else {
                        $types .= \chr($type);
                    }
                    $types .= $unsigned?"\x80":"\0";
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
            $this->packetCallback = [$this, "handleExecute"];
        });
        return $deferred->promise(); // do not use $this->startCommand(), that might unexpectedly reset the seqId!
    }

    /** @see 14.7.7 COM_STMT_CLOSE */
    public function closeStmt(int $stmtId)
    {
        $payload = "\x19" . DataTypes::encodeInt32($stmtId);
        $this->appendTask(function () use ($payload) {
            if ($this->connectionState === self::READY) {
                $this->resetIds();
                $this->sendPacket($payload);
                $this->resetIds(); // does not expect a reply - must be reset immediately
            }
            $this->ready();
        });
    }

    /** @see 14.6.5 COM_FIELD_LIST */
    public function listFields(string $table, string $like = "%"): Promise
    {
        return $this->startCommand(static function () use ($table, $like) {
            $this->sendPacket("\x04$table\0$like");
            $this->parseCallback = [$this, "handleFieldlist"];
        });
    }

    public function listAllFields(string $table, string $like = "%"): Promise
    {
        $deferred = new Deferred;

        $columns = [];
        $onResolve = function ($error, $array) use (&$columns, &$onResolve, $deferred) {
            if ($error) {
                $deferred->fail($error);
                return;
            }
            if ($array === null) {
                $deferred->resolve($columns);
                return;
            }
            list($columns[], $promise) = $array;
            $promise->onResolve($onResolve);
        };
        $this->listFields($table, $like)->onResolve($onResolve);

        return $deferred->promise();
    }

    /** @see 14.6.6 COM_CREATE_DB */
    public function createDatabase($db)
    {
        return $this->startCommand(function () use ($db) {
            $this->sendPacket("\x05$db");
        });
    }

    /** @see 14.6.7 COM_DROP_DB */
    public function dropDatabase(string $db): Promise
    {
        return $this->startCommand(function () use ($db) {
            $this->sendPacket("\x06$db");
        });
    }

    /**
     * @param $subcommand int one of the self::REFRESH_* constants
     * @see 14.6.8 COM_REFRESH
     */
    public function refresh(int $subcommand): Promise
    {
        return $this->startCommand(function () use ($subcommand) {
            $this->sendPacket("\x07" . \chr($subcommand));
        });
    }

    /** @see 14.6.9 COM_SHUTDOWN */
    public function shutdown(): Promise
    {
        return $this->startCommand(function () {
            $this->sendPacket("\x08\x00"); /* SHUTDOWN_DEFAULT / SHUTDOWN_WAIT_ALL_BUFFERS, only one in use */
        });
    }

    /** @see 14.6.10 COM_STATISTICS */
    public function statistics(): Promise
    {
        return $this->startCommand(function () {
            $this->sendPacket("\x09");
            $this->parseCallback = [$this, "readStatistics"];
        });
    }

    /** @see 14.6.11 COM_PROCESS_INFO */
    public function processInfo(): Promise
    {
        return $this->startCommand(function () {
            $this->sendPacket("\x0a");
            $this->query("SHOW PROCESSLIST");
        });
    }

    /** @see 14.6.13 COM_PROCESS_KILL */
    public function killProcess($process): Promise
    {
        return $this->startCommand(function () use ($process) {
            $this->sendPacket("\x0c" . DataTypes::encodeInt32($process));
        });
    }

    /** @see 14.6.14 COM_DEBUG */
    public function debugStdout(): Promise
    {
        return $this->startCommand(function () {
            $this->sendPacket("\x0d");
        });
    }

    /** @see 14.7.8 COM_STMT_RESET */
    public function resetStmt(int $stmtId): Promise
    {
        $payload = "\x1a" . DataTypes::encodeInt32($stmtId);
        $deferred = new Deferred;
        $this->appendTask(function () use ($payload, $deferred) {
            $this->resetIds();
            $this->addDeferred($deferred);
            $this->sendPacket($payload);
        });
        return $deferred->promise();
    }

    /** @see 14.8.4 COM_STMT_FETCH */
    public function fetchStmt(int $stmtId): Promise
    {
        $payload = "\x1c" . DataTypes::encodeInt32($stmtId) . DataTypes::encodeInt32(1);
        $deferred = new Deferred;
        $this->appendTask(function () use ($payload, $deferred) {
            $this->resetIds();
            $this->addDeferred($deferred);
            $this->sendPacket($payload);
        });
        return $deferred->promise();
    }

    private function established()
    {
        // @TODO flags to use?
        $this->capabilities |= self::CLIENT_SESSION_TRACK | self::CLIENT_TRANSACTIONS | self::CLIENT_PROTOCOL_41 | self::CLIENT_SECURE_CONNECTION | self::CLIENT_MULTI_RESULTS | self::CLIENT_PS_MULTI_RESULTS | self::CLIENT_MULTI_STATEMENTS | self::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA;

        if (\extension_loaded("zlib") && $this->config->isCompressionEnabled()) {
            $this->capabilities |= self::CLIENT_COMPRESS;
        }

        if ($this->config->isLocalInfileEnabled()) {
            $this->capabilities |=  self::CLIENT_LOCAL_INFILE;
        }
    }

    /** @see 14.1.3.2 ERR-Packet */
    private function handleError($packet)
    {
        $off = 1;

        $this->connInfo->errorCode = DataTypes::decodeUnsigned16(\substr($packet, $off, 2));
        $off += 2;

        if ($this->capabilities & self::CLIENT_PROTOCOL_41) {
            $this->connInfo->errorState = \substr($packet, $off, 6);
            $off += 6;
        }

        $this->connInfo->errorMsg = \substr($packet, $off);

        $this->parseCallback = null;

        if ($this->connectionState < self::READY) {
            // connection failure
            $this->close();
            $this->getDeferred()->fail(new InitializationException("Could not connect to {$this->config->getConnectionString()}: {$this->connInfo->errorState} {$this->connInfo->errorMsg}"));
            return;
        }

        if ($this->result === null && empty($this->deferreds)) {
            // connection killed without pending query or active result
            $this->close();
            return;
        }

        $deferred = $this->result ?? $this->getDeferred();

        // normal error
        $deferred->fail(new QueryError("MySQL error ({$this->connInfo->errorCode}): {$this->connInfo->errorState} {$this->connInfo->errorMsg}", $this->query));
        $this->result = null;
        $this->query = null;
        $this->ready();
    }

    /** @see 14.1.3.1 OK-Packet */
    private function parseOk($packet)
    {
        $off = 1;

        $this->connInfo->affectedRows = DataTypes::decodeUnsigned(\substr($packet, $off), $intlen);
        $off += $intlen;

        $this->connInfo->insertId = DataTypes::decodeUnsigned(\substr($packet, $off), $intlen);
        $off += $intlen;

        if ($this->capabilities & (self::CLIENT_PROTOCOL_41 | self::CLIENT_TRANSACTIONS)) {
            $this->connInfo->statusFlags = DataTypes::decodeUnsigned16(\substr($packet, $off));
            $off += 2;

            $this->connInfo->warnings = DataTypes::decodeUnsigned16(\substr($packet, $off));
            $off += 2;
        }

        if ($this->capabilities & self::CLIENT_SESSION_TRACK) {
            // Even though it seems required according to 14.1.3.1, there is no length encoded string, i.e. no trailing NULL byte ....???
            if (\strlen($packet) > $off) {
                $this->connInfo->statusInfo = DataTypes::decodeStringOff(DataTypes::MYSQL_TYPE_STRING, $packet, $off);

                if ($this->connInfo->statusFlags & StatusFlags::SERVER_SESSION_STATE_CHANGED) {
                    $sessionState = DataTypes::decodeString(\substr($packet, $off), $intlen, $sessionStateLen);
                    $len = 0;
                    while ($len < $sessionStateLen) {
                        $data = DataTypes::decodeString(\substr($sessionState, $len + 1), $datalen, $intlen);

                        switch ($type = DataTypes::decodeUnsigned8(\substr($sessionState, $len))) {
                            case SessionStateTypes::SESSION_TRACK_SYSTEM_VARIABLES:
                                $var = DataTypes::decodeString($data, $varintlen, $strlen);
                                $this->connInfo->sessionState[SessionStateTypes::SESSION_TRACK_SYSTEM_VARIABLES][$var] = DataTypes::decodeString(\substr($data, $varintlen + $strlen));
                                break;
                            case SessionStateTypes::SESSION_TRACK_SCHEMA:
                                $this->connInfo->sessionState[SessionStateTypes::SESSION_TRACK_SCHEMA] = DataTypes::decodeString($data);
                                break;
                            case SessionStateTypes::SESSION_TRACK_STATE_CHANGE:
                                $this->connInfo->sessionState[SessionStateTypes::SESSION_TRACK_STATE_CHANGE] = DataTypes::decodeString($data);
                                break;
                            default:
                                throw new \Error("$type is not a valid mysql session state type");
                        }

                        $len += 1 + $intlen + $datalen;
                    }
                }
            } else {
                $this->connInfo->statusInfo = "";
            }
        } else {
            $this->connInfo->statusInfo = \substr($packet, $off);
        }
    }

    private function handleOk($packet)
    {
        $this->parseOk($packet);
        $this->getDeferred()->resolve(new CommandResult($this->connInfo->affectedRows, $this->connInfo->insertId));
        $this->ready();
    }

    /** @see 14.1.3.3 EOF-Packet */
    private function parseEof($packet)
    {
        if ($this->capabilities & self::CLIENT_PROTOCOL_41) {
            $this->connInfo->warnings = DataTypes::decodeUnsigned16(\substr($packet, 1));

            $this->connInfo->statusFlags = DataTypes::decodeUnsigned16(\substr($packet, 3));
        }
    }

    private function handleEof($packet)
    {
        $this->parseEof($packet);
        $exception = new FailureException($this->connInfo->errorMsg, $this->connInfo->errorCode);
        $this->getDeferred()->fail($exception);
        $this->ready();
    }

    /** @see 14.2.5 Connection Phase Packets */
    private function handleHandshake($packet)
    {
        $off = 1;

        $this->protocol = \ord($packet);
        if ($this->protocol !== 0x0a) {
            throw new ConnectionException("Unsupported protocol version ".\ord($packet)." (Expected: 10)");
        }

        $this->connInfo->serverVersion = DataTypes::decodeNullString(\substr($packet, $off), $len);
        $off += $len + 1;

        $this->connectionId = DataTypes::decodeUnsigned32(\substr($packet, $off));
        $off += 4;

        $this->authPluginData = \substr($packet, $off, 8);
        $off += 8;

        $off += 1; // filler byte

        $this->serverCapabilities = DataTypes::decodeUnsigned16(\substr($packet, $off));
        $off += 2;

        if (\strlen($packet) > $off) {
            $this->connInfo->charset = \ord(\substr($packet, $off));
            $off += 1;

            $this->connInfo->statusFlags = DataTypes::decodeUnsigned16(\substr($packet, $off));
            $off += 2;

            $this->serverCapabilities += DataTypes::decodeUnsigned16(\substr($packet, $off)) << 16;
            $off += 2;

            $this->authPluginDataLen = $this->serverCapabilities & self::CLIENT_PLUGIN_AUTH ? \ord(\substr($packet, $off)) : 0;
            $off += 1;

            if ($this->serverCapabilities & self::CLIENT_SECURE_CONNECTION) {
                $off += 10;

                $strlen = \max(13, $this->authPluginDataLen - 8);
                $this->authPluginData .= \substr($packet, $off, $strlen);
                $off += $strlen;

                if ($this->serverCapabilities & self::CLIENT_PLUGIN_AUTH) {
                    $this->authPluginName = DataTypes::decodeNullString(\substr($packet, $off));
                }
            }
        }

        $this->sendHandshake();
    }

    /** @see 14.6.4.1.2 LOCAL INFILE Request */
    private function handleLocalInfileRequest($packet)
    {
        \Amp\asyncCall(function () use ($packet) {
            try {
                $filePath = \substr($packet, 1);
                /** @var \Amp\File\Handle $fileHandle */
                $fileHandle = yield \Amp\File\open($filePath, 'r');
                while ("" != ($chunk = yield $fileHandle->read())) {
                    $this->sendPacket($chunk);
                }
                $this->sendPacket("");
            } catch (\Throwable $e) {
                $this->getDeferred()->fail(new ConnectionException("Failed to transfer a file to the server", 0, $e));
            }
        });
    }

    /** @see 14.6.4.1.1 Text Resultset */
    private function handleQuery($packet)
    {
        switch (\ord($packet)) {
            case self::OK_PACKET:
                $this->parseOk($packet);
                if ($this->connInfo->statusFlags & StatusFlags::SERVER_MORE_RESULTS_EXISTS) {
                    $this->getDeferred()->resolve($result = new ResultProxy);
                    $this->result = $result;
                    $result->updateState(ResultProxy::COLUMNS_FETCHED);
                    $this->successfulResultsetFetch();
                } else {
                    $this->parseCallback = null;
                    $this->getDeferred()->resolve(new CommandResult($this->connInfo->affectedRows, $this->connInfo->insertId));
                    $this->ready();
                }
                return;
            case self::LOCAL_INFILE_REQUEST:
                if ($this->config->isLocalInfileEnabled()) {
                    $this->handleLocalInfileRequest($packet);
                } else {
                    $this->getDeferred()->fail(new ConnectionException("Unexpected LOCAL_INFILE_REQUEST packet"));
                }
                return;
            case self::ERR_PACKET:
                $this->handleError($packet);
                return;
        }

        $this->parseCallback = [$this, "handleTextColumnDefinition"];
        $this->getDeferred()->resolve($result = new ResultProxy);
        /* we need to resolve before assigning vars, so that a onResolve() handler won't have a partial result available */
        $this->result = $result;
        $result->setColumns(DataTypes::decodeUnsigned($packet));
    }

    /** @see 14.7.1 Binary Protocol Resultset */
    private function handleExecute($packet)
    {
        $this->parseCallback = [$this, "handleBinaryColumnDefinition"];
        $this->getDeferred()->resolve($result = new ResultProxy);
        /* we need to resolve before assigning vars, so that a onResolve() handler won't have a partial result available */
        $this->result = $result;
        $result->setColumns(\ord($packet));
    }

    private function handleFieldList($packet)
    {
        if (\ord($packet) === self::ERR_PACKET) {
            $this->parseCallback = null;
            $this->handleError($packet);
        } elseif (\ord($packet) === self::EOF_PACKET) {
            $this->parseCallback = null;
            $this->parseEof($packet);
            $this->getDeferred()->resolve(null);
            $this->ready();
        } else {
            $this->addDeferred($deferred = new Deferred);
            $this->getDeferred()->resolve([$this->parseColumnDefinition($packet), $deferred]);
        }
    }

    private function handleTextColumnDefinition($packet)
    {
        $this->handleColumnDefinition($packet, "handleTextResultsetRow");
    }

    private function handleBinaryColumnDefinition($packet)
    {
        $this->handleColumnDefinition($packet, "handleBinaryResultsetRow");
    }

    private function handleColumnDefinition($packet, $cbMethod)
    {
        if (!$this->result->columnsToFetch--) {
            $this->result->updateState(ResultProxy::COLUMNS_FETCHED);
            if (\ord($packet) === self::ERR_PACKET) {
                $this->parseCallback = null;
                $this->handleError($packet);
            } else {
                $cb = $this->parseCallback = [$this, $cbMethod];
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

    private function prepareParams($packet)
    {
        if (!$this->result->columnsToFetch--) {
            $this->result->columnsToFetch = $this->result->columnCount;
            if (!$this->result->columnsToFetch) {
                $this->prepareFields($packet);
            } else {
                $this->parseCallback = [$this, "prepareFields"];
            }
            return;
        }

        $this->result->params[] = $this->parseColumnDefinition($packet);
    }

    private function prepareFields($packet)
    {
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
    private function parseColumnDefinition($packet)
    {
        $off = 0;

        $column = [];

        if ($this->capabilities & self::CLIENT_PROTOCOL_41) {
            $column["catalog"] = DataTypes::decodeStringOff(DataTypes::MYSQL_TYPE_STRING, $packet, $off);
            $column["schema"] = DataTypes::decodeStringOff(DataTypes::MYSQL_TYPE_STRING, $packet, $off);
            $column["table"] = DataTypes::decodeStringOff(DataTypes::MYSQL_TYPE_STRING, $packet, $off);
            $column["original_table"] = DataTypes::decodeStringOff(DataTypes::MYSQL_TYPE_STRING, $packet, $off);
            $column["name"] = DataTypes::decodeStringOff(DataTypes::MYSQL_TYPE_STRING, $packet, $off);
            $column["original_name"] = DataTypes::decodeStringOff(DataTypes::MYSQL_TYPE_STRING, $packet, $off);
            $fixlen = DataTypes::decodeUnsignedOff($packet, $off);

            $len = 0;
            $column["charset"] = DataTypes::decodeUnsigned16(\substr($packet, $off + $len));
            $len += 2;
            $column["columnlen"] = DataTypes::decodeUnsigned32(\substr($packet, $off + $len));
            $len += 4;
            $column["type"] = \ord($packet[$off + $len]);
            $len += 1;
            $column["flags"] = DataTypes::decodeUnsigned16(\substr($packet, $off + $len));
            $len += 2;
            $column["decimals"] = \ord($packet[$off + $len]);
            //$len += 1;

            $off += $fixlen;
        } else {
            $column["table"] = DataTypes::decodeStringOff(DataTypes::MYSQL_TYPE_STRING, $packet, $off);
            $column["name"] = DataTypes::decodeStringOff(DataTypes::MYSQL_TYPE_STRING, $packet, $off);

            $collen = DataTypes::decodeUnsignedOff($packet, $off);
            $column["columnlen"] = DataTypes::decodeIntByLen(\substr($packet, $off), $collen);
            $off += $collen;

            $typelen = DataTypes::decodeUnsignedOff($packet, $off);
            $column["type"] = DataTypes::decodeIntByLen(\substr($packet, $off), $typelen);
            $off += $typelen;

            $len = 1;
            $flaglen = $this->capabilities & self::CLIENT_LONG_FLAG ? DataTypes::decodeUnsigned(\substr($packet, $off, 9), $len) : \ord($packet[$off]);
            $off += $len;

            if ($flaglen > 2) {
                $len = 2;
                $column["flags"] = DataTypes::decodeUnsigned16(\substr($packet, $off, 4));
            } else {
                $len = 1;
                $column["flags"] = \ord($packet[$off]);
            }
            $column["decimals"] = \ord($packet[$off + $len]);
            $off += $flaglen;
        }

        if ($off < \strlen($packet)) {
            $column["defaults"] = DataTypes::decodeString(\substr($packet, $off));
        }

        return $column;
    }

    private function successfulResultsetFetch()
    {
        $result = $this->result;
        $deferred = &$result->next;
        if ($this->connInfo->statusFlags & StatusFlags::SERVER_MORE_RESULTS_EXISTS) {
            $this->parseCallback = [$this, "handleQuery"];
            $this->addDeferred($deferred ?: $deferred = new Deferred);
        } else {
            if (!$deferred) {
                $deferred = new Deferred;
            }
            $deferred->resolve();
            $this->parseCallback = null;
            $this->query = null;
            $this->result = null;
            $this->ready();
        }
        $result->updateState(ResultProxy::ROWS_FETCHED);
    }

    /** @see 14.6.4.1.1.3 Resultset Row */
    private function handleTextResultsetRow($packet)
    {
        $packettype = \ord($packet);
        if ($packettype === self::EOF_PACKET) {
            if ($this->capabilities & self::CLIENT_DEPRECATE_EOF) {
                $this->parseOk($packet);
            } else {
                $this->parseEof($packet);
            }
            $this->successfulResultsetFetch();
            return;
        } elseif ($packettype === self::ERR_PACKET) {
            $this->handleError($packet);
            return;
        }

        $off = 0;
        $columns = $this->result->columns;

        $fields = [];
        for ($i = 0; $off < \strlen($packet); ++$i) {
            if (\ord($packet[$off]) === 0xfb) {
                $fields[] = null;
                $off += 1;
            } else {
                $fields[] = DataTypes::decodeStringOff($columns[$i]["type"], $packet, $off);
            }
        }
        $this->result->rowFetched($fields);
    }

    /** @see 14.7.2 Binary Protocol Resultset Row */
    private function handleBinaryResultsetRow($packet)
    {
        $packettype = \ord($packet);
        if ($packettype === self::EOF_PACKET) {
            $this->parseEof($packet);
            $this->successfulResultsetFetch();
            return;
        } elseif ($packettype === self::ERR_PACKET) {
            $this->handleError($packet);
            return;
        }

        $off = 1; // skip first byte

        $columnCount = $this->result->columnCount;
        $columns = $this->result->columns;
        $fields = [];

        for ($i = 0; $i < $columnCount; $i++) {
            if (\ord($packet[$off + (($i + 2) >> 3)]) & (1 << (($i + 2) % 8))) {
                $fields[$i] = null;
            }
        }
        $off += ($columnCount + 9) >> 3;

        for ($i = 0; $off < \strlen($packet); $i++) {
            while (\array_key_exists($i, $fields)) {
                $i++;
            }
            $fields[$i] = DataTypes::decodeBinary($columns[$i]["type"], \substr($packet, $off), $len);
            $off += $len;
        }
        \ksort($fields);
        $this->result->rowFetched($fields);
    }

    /** @see 14.7.4.1 COM_STMT_PREPARE Response */
    private function handlePrepare($packet)
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
        $off = 1;

        $stmtId = DataTypes::decodeUnsigned32(\substr($packet, $off));
        $off += 4;

        $columns = DataTypes::decodeUnsigned16(\substr($packet, $off));
        $off += 2;

        $params = DataTypes::decodeUnsigned16(\substr($packet, $off));
        $off += 2;

        $off += 1; // filler

        $this->connInfo->warnings = DataTypes::decodeUnsigned16(\substr($packet, $off));

        $this->result = new ResultProxy;
        $this->result->columnsToFetch = $params;
        $this->result->columnCount = $columns;
        $this->refcount++;
        $this->getDeferred()->resolve(new ConnectionStatement($this, $this->query, $stmtId, $this->named, $this->result));
        $this->named = [];
        if ($params) {
            $this->parseCallback = [$this, "prepareParams"];
        } else {
            $this->prepareParams($packet);
        }
    }

    private function readStatistics($packet)
    {
        $this->getDeferred()->resolve($packet);
        $this->ready();
        $this->parseCallback = null;
    }

    /** @see 14.6.2 COM_QUIT */
    public function sendClose(): Promise
    {
        return $this->startCommand(function () {
            $this->sendPacket("\x01");
            $this->connectionState = self::CLOSING;
        });
    }

    public function close()
    {
        if ($this->connectionState === self::CLOSING && $this->deferreds) {
            \array_pop($this->deferreds)->resolve();
        }

        $this->connectionState = self::CLOSED;

        if ($this->socket) {
            $this->socket->close();
            $this->socket = null;
        }
    }

    private function write(string $packet): Promise
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

        return $this->pendingWrite = $this->socket->write($packet);
    }

    private function resetIds()
    {
        if ($this->pendingWrite) {
            $this->pendingWrite->onResolve(function () {
                $this->seqId = $this->compressionId = -1;
            });
            return;
        }

        $this->seqId = $this->compressionId = -1;
    }

    private function compilePacket(string $pending): string
    {
        $packet = "";
        do {
            $len = \strlen($pending);
            if ($len >= (1 << 24) - 1) {
                $out = \substr($pending, 0, (1 << 24) - 1);
                $pending = \substr($pending, (1 << 24) - 1);
                $len = (1 << 24) - 1;
            } else {
                $out = $pending;
                $pending = "";
            }
            $packet .= \substr_replace(\pack("V", $len), \chr(++$this->seqId), 3, 1) . $out; // expects $len < (1 << 24) - 1
        } while ($pending !== "");

        return $packet;
    }

    private function compressPacket(string $packet): string
    {
        if ($packet === "") {
            return "";
        }

        $len = \strlen($packet);
        $deflated = \zlib_encode($packet, ZLIB_ENCODING_DEFLATE);

        if ($len < \strlen($deflated)) {
            return \substr_replace(\pack("V", \strlen($packet)), \chr(++$this->compressionId), 3, 1) . "\0\0\0" . $packet;
        }

        return \substr_replace(\pack("V", \strlen($deflated)), \chr(++$this->compressionId), 3, 1) . \substr(\pack("V", $len), 0, 3) . $deflated;
    }

    /** @see 14.4 Compression */
    private function parseCompression(): \Generator
    {
        $inflated = "";
        $buf = "";

        while (true) {
            while (\strlen($buf) < 7) {
                $buf .= yield $inflated;
                $inflated = "";
            }

            $size = DataTypes::decodeUnsigned24($buf);
            $this->compressionId = \ord($buf[3]);
            $uncompressed = DataTypes::decodeUnsigned24(\substr($buf, 4, 3));

            $buf = \substr($buf, 7);

            if ($size > 0) {
                while (\strlen($buf) < $size) {
                    $buf .= yield $inflated;
                    $inflated = "";
                }

                if ($uncompressed === 0) {
                    $inflated .= \substr($buf, 0, $size);
                } else {
                    $inflated .= \zlib_decode(\substr($buf, 0, $size), $uncompressed);
                }

                $buf = \substr($buf, $size);
            }
        }
    }

    /**
     * @see 14.1.2 MySQL Packet
     * @see 14.1.3 Generic Response Packets
     */
    private function parseMysql(): \Generator
    {
        $buf = "";
        $parsed = [];

        while (true) {
            $packet = "";

            do {
                while (\strlen($buf) < 4) {
                    $buf .= yield $parsed;
                    $parsed = [];
                }

                $len = DataTypes::decodeUnsigned24($buf);
                $this->seqId = \ord($buf[3]);
                $buf = \substr($buf, 4);

                while (\strlen($buf) < ($len & 0xffffff)) {
                    $buf .= yield $parsed;
                    $parsed = [];
                }

                $lastIn = $len !== 0xffffff;
                if ($lastIn) {
                    $size = $len % 0xffffff;
                } else {
                    $size = 0xffffff;
                }

                $packet .= \substr($buf, 0, $size);
                $buf = \substr($buf, $size);
            } while (!$lastIn);

            if (\strlen($packet) > 0) {
                $parsed[] = $packet;
            }
        }
    }

    private function parsePayload($packet)
    {
        if ($this->connectionState === self::UNCONNECTED) {
            $this->established();
            $this->connectionState = self::ESTABLISHED;
            $this->handleHandshake($packet);
        } elseif ($this->connectionState === self::ESTABLISHED) {
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
                case self::EXTRA_AUTH_PACKET:
                    /** @see 14.2.5 Connection Phase Packets (AuthMoreData) */
                    switch ($this->authPluginName) {
                        case "sha256_password":
                            $key = \substr($packet, 1);
                            $this->config = $this->config->withKey($key);
                            $this->sendHandshake();
                            break;
                        default:
                            throw new ConnectionException("Unexpected EXTRA_AUTH_PACKET in authentication phase for method {$this->authPluginName}");
                    }
                    break;
            }
        } else {
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
                    if ($cb) {
                        $cb($packet);
                    } else {
                        throw new ConnectionException("Unexpected packet type: " . \ord($packet));
                    }
            }
        }
    }

    private function secureAuth($pass, $scramble)
    {
        $hash = \sha1($pass, 1);
        return $hash ^ \sha1(\substr($scramble, 0, 20) . \sha1($hash, 1), 1);
    }

    private function sha256Auth($pass, $scramble, $key)
    {
        \openssl_public_encrypt($pass ^ \str_repeat($scramble, \ceil(\strlen($pass) / \strlen($scramble))), $auth, $key, OPENSSL_PKCS1_OAEP_PADDING);
        return $auth;
    }

    private function authSwitchRequest($packet)
    {
        $this->parseCallback = null;
        switch (\ord($packet)) {
            case self::EOF_PACKET:
                if (\strlen($packet) === 1) {
                    break;
                }
                $len = \strpos($packet, "\0");
                $pluginName = \substr($packet, 0, $len); // @TODO mysql_native_pass only now...
                $authPluginData = \substr($packet, $len + 1);
                $this->sendPacket($this->secureAuth($this->config->getPassword(), $authPluginData));
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
    private function sendHandshake($inSSL = false)
    {
        if ($this->config->getDatabase() !== null) {
            $this->capabilities |= self::CLIENT_CONNECT_WITH_DB;
        }

        if ($this->config->getTlsContext() !== null) {
            $this->capabilities |= self::CLIENT_SSL;
        }

        $this->capabilities &= $this->serverCapabilities;

        $payload = "";
        $payload .= \pack("V", $this->capabilities);
        $payload .= \pack("V", 1 << 24 - 1); // max-packet size
        $payload .= \chr(ConnectionConfig::BIN_CHARSET);
        $payload .= \str_repeat("\0", 23); // reserved

        if (!$inSSL && ($this->capabilities & self::CLIENT_SSL)) {
            \Amp\asyncCall(function () use ($payload) {
                try {
                    yield $this->write($payload);

                    $context = $this->config->getTlsContext() ?: new ClientTlsContext;
                    $context = $context->withPeerName($this->config->getHost());

                    yield $this->socket->enableCrypto($context);

                    $this->sendHandshake(true);
                } catch (\Throwable $e) {
                    $this->close();
                    $this->getDeferred()->fail($e);
                }
            });

            return;
        }

        $payload .= $this->config->getUser()."\0";
        if ($this->config->getPassword() == "") {
            $auth = "";
        } elseif ($this->capabilities & self::CLIENT_PLUGIN_AUTH) {
            switch ($this->authPluginName) {
                case "mysql_native_password":
                    $auth = $this->secureAuth($this->config->getPassword(), $this->authPluginData);
                    break;
                case "mysql_clear_password":
                    $auth = $this->config->getPassword();
                    break;
                case "sha256_password":
                    if ($this->config->getPassword() === "") {
                        $auth = "";
                    } else {
                        $key = $this->config->getKey();
                        if ($key !== null) {
                            $auth = $this->sha256Auth($this->config->getPassword(), $this->authPluginData, $key);
                        } else {
                            $auth = "\x01";
                        }
                    }
                    break;
                case "mysql_old_password":
                    throw new ConnectionException("mysql_old_password is outdated and insecure. Intentionally not implemented!");
                default:
                    throw new ConnectionException("Invalid (or unimplemented?) auth method requested by server: {$this->authPluginName}");
            }
        } else {
            $auth = $this->secureAuth($this->config->getPassword(), $this->authPluginData);
        }
        if ($this->capabilities & self::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) {
            $payload .= DataTypes::encodeInt(\strlen($auth));
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
            $payload .= "\0"; // @TODO AUTH
//            $payload .= "mysql_native_password\0";
        }
        if ($this->capabilities & self::CLIENT_CONNECT_ATTRS) {
            // connection attributes?! 5.6.6+ only!
        }
        $this->write($payload);
    }

    /** @see 14.1.2 MySQL Packet */
    protected function sendPacket(string $payload): Promise
    {
        if ($this->connectionState !== self::READY) {
            throw new \Error("Connection not ready, cannot send any packets");
        }

        return $this->write($payload);
    }
}
