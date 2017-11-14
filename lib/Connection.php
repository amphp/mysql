<?php

namespace Amp\Mysql;

use Amp\Deferred;
use Amp\Promise;

class Connection implements Link {
    const REFRESH_GRANT = 0x01;
    const REFRESH_LOG = 0x02;
    const REFRESH_TABLES = 0x04;
    const REFRESH_HOSTS = 0x08;
    const REFRESH_STATUS = 0x10;
    const REFRESH_THREADS = 0x20;
    const REFRESH_SLAVE = 0x40;
    const REFRESH_MASTER = 0x80;

    /** @var \Amp\Mysql\Internal\Processor */
    private $processor;

    public static function connect(ConnectionConfig $config): Promise {
        $processor = new Internal\Processor($config);

        return \Amp\call(function () use ($config, $processor) {
            yield $processor->connect();
            return new self($processor);
        });
    }

    public function __construct(Internal\Processor $processor) {
        $this->processor = $processor;
    }

    public function isAlive(): bool {
        return $this->processor->isAlive();
    }

    public function isReady(): bool {
        return $this->processor->isReady();
    }

    protected function forceClose() {
        $this->processor->close();
    }

    public function getConfig(): ConnectionConfig {
        return clone $this->processor->config;
    }

    public function getConnInfo(): ConnectionState {
        return $this->processor->getConnInfo();
    }

    public function setCharset(string $charset, string $collate = ""): Promise {
        if ($collate === "" && false !== $off = strpos($charset, "_")) {
            $collate = $charset;
            $charset = substr($collate, 0, $off);
        }

        $this->processor->config->charset = $charset;
        $this->processor->config->collate = $collate;

        $query = "SET NAMES '$charset'".($collate == "" ? "" : " COLLATE '$collate'");
        return $this->query($query);
    }

    /** @see 14.6.2 COM_QUIT */
    public function close() {
        $processor = $this->processor;
        $processor->startCommand(static function () use ($processor) {
            $processor->sendPacket("\x01");
            $processor->initClosing();
        })->onResolve(static function () use ($processor) {
            $processor->close();
        });
    }

    /** @see 14.6.3 COM_INIT_DB */
    public function useDb(string $db): Promise {
        $processor = $this->processor;
        return $processor->startCommand(static function () use ($processor, $db) {
            $processor->config->db = $db;
            $processor->sendPacket("\x02$db");
        });
    }

    /** @see 14.6.4 COM_QUERY */
    public function query(string $query): Promise {
        $processor = $this->processor;
        return \Amp\call(static function () use ($processor, $query) {
            $result = yield $processor->startCommand(static function () use ($processor, $query) {
                $processor->setQuery($query);
                $processor->sendPacket("\x03$query");
            });

            if ($result instanceof Internal\ResultProxy) {
                return new ResultSet($result);
            }

            if ($result instanceof ConnectionState) {
                return new CommandResult($result->affectedRows, $result->insertId);
            }

            throw new FailureException("Unrecognized result type");
        });
    }

    public function transaction(int $isolation = Transaction::COMMITTED): Promise {
        return \Amp\call(function () use ($isolation) {
            switch ($isolation) {
                case Transaction::UNCOMMITTED:
                    yield $this->query("BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
                    break;

                case Transaction::COMMITTED:
                    yield $this->query("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED");
                    break;

                case Transaction::REPEATABLE:
                    yield $this->query("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ");
                    break;

                case Transaction::SERIALIZABLE:
                    yield $this->query("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE");
                    break;

                default:
                    throw new \Error("Invalid transaction type");
            }

            return new Transaction($this);
        });
    }

    /** @see 14.6.5 COM_FIELD_LIST */
    public function listFields(string $table, string $like = "%"): Promise {
        $processor = $this->processor;
        return $processor->startCommand(static function () use ($processor, $table, $like) {
            $processor->sendPacket("\x04$table\0$like");
            $processor->setFieldListing();
        });
    }

    public function listAllFields(string $table, string $like = "%"): Promise {
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
    public function createDatabase($db) {
        $processor = $this->processor;
        return $processor->startCommand(static function () use ($processor, $db) {
            $processor->sendPacket("\x05$db");
        });
    }

    /** @see 14.6.7 COM_DROP_DB */
    public function dropDatabase(string $db): Promise {
        $processor = $this->processor;
        return $processor->startCommand(static function () use ($processor, $db) {
            $processor->sendPacket("\x06$db");
        });
    }

    /**
     * @param $subcommand int one of the self::REFRESH_* constants
     * @see 14.6.8 COM_REFRESH
     */
    public function refresh(int $subcommand): Promise {
        $processor = $this->processor;
        return $processor->startCommand(static function () use ($processor, $subcommand) {
            $processor->sendPacket("\x07" . chr($subcommand));
        });
    }

    /** @see 14.6.9 COM_SHUTDOWN */
    public function shutdown(): Promise {
        $processor = $this->processor;
        return $processor->startCommand(static function () use ($processor) {
            $processor->sendPacket("\x08\x00"); /* SHUTDOWN_DEFAULT / SHUTDOWN_WAIT_ALL_BUFFERS, only one in use */
        });
    }

    /** @see 14.6.10 COM_STATISTICS */
    public function statistics(): Promise {
        $processor = $this->processor;
        return $processor->startCommand(static function () use ($processor) {
            $processor->sendPacket("\x09");
            $processor->setStatisticsReading();
        });
    }

    /** @see 14.6.11 COM_PROCESS_INFO */
    public function processInfo(): Promise {
        $processor = $this->processor;
        return $processor->startCommand(static function () use ($processor) {
            $processor->sendPacket("\x0a");
            $processor->setQuery("SHOW PROCESSLIST");
        });
    }

    /** @see 14.6.13 COM_PROCESS_KILL */
    public function killProcess($process): Promise {
        $processor = $this->processor;
        return $processor->startCommand(static function () use ($processor, $process) {
            $processor->sendPacket("\x0c" . DataTypes::encode_int32($process));
        });
    }

    /** @see 14.6.14 COM_DEBUG */
    public function debugStdout(): Promise {
        $processor = $this->processor;
        return $processor->startCommand(static function () use ($processor) {
            $processor->sendPacket("\x0d");
        });
    }

    /** @see 14.6.15 COM_PING */
    public function ping(): Promise {
        $processor = $this->processor;
        return $processor->startCommand(static function () use ($processor) {
            $processor->sendPacket("\x0e");
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

    /** @see 14.6.19 COM_RESET_CONNECTION */
    public function resetConnection() {
        $processor = $this->processor;
        return $processor->startCommand(static function () use ($processor) {
            $processor->sendPacket("\x1f");
        });
    }

    /** @see 14.7.4 COM_STMT_PREPARE */
    public function prepare(string $query): Promise {
        $processor = $this->processor;
        $promise = $processor->startCommand(static function () use ($processor, $query) {
            $processor->setPrepare($query);
            $regex = <<<'REGEX'
(["'`])(?:\\(?:\\|\1)|(?!\1).)*+\1(*SKIP)(*F)|(\?)|:([a-zA-Z_]+)
REGEX;

            $index = 0;
            $query = preg_replace_callback("~$regex~ms", function ($m) use ($processor, &$index) {
                if ($m[2] !== "?") {
                    $processor->named[$m[3]][] = $index;
                }
                $index++;
                return "?";
            }, $query);
            $processor->sendPacket("\x16$query");
        });

        return $promise;
    }

    /**
     * {@inheritdoc}
     */
    public function execute(string $sql, ...$data): Promise {
        return \Amp\call(function () use ($sql, $data) {
            /** @var \Amp\Mysql\Statement $statment */
            $statment = yield $this->prepare($sql);
            return yield $statment->execute(...$data);
        });
    }

    public function __destruct() {
        $this->processor->delRef();
    }
}
