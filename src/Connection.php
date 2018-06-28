<?php

namespace Amp\Mysql;

use Amp\Deferred;
use Amp\Promise;
use Amp\Socket\Socket;
use Amp\Sql\Connection as SqlConnection;
use Amp\Sql\FailureException;
use function Amp\call;

final class Connection implements SqlConnection {
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

    /** @var \Amp\Deferred|null */
    private $busy;

    public function __construct(Socket $socket, ConnectionConfig $config) {
        $this->processor = new Internal\Processor($socket, $config);
    }

    public function connect(): Promise {
        return call(function () {
            yield $this->processor->connect();
        });
    }

    /**
     * @return bool False if the connection has been closed.
     */
    public function isAlive(): bool {
        return $this->processor && $this->processor->isAlive();
    }

    /**
     * @return int Timestamp of the last time this connection was used.
     *
     * @throws FailureException
     */
    public function lastUsedAt(): int {
        if (! $this->processor) {
            throw new FailureException('Not connected');
        }

        return $this->processor->lastDataAt();
    }

    public function isReady(): bool {
        return $this->processor && $this->processor->isReady();
    }

    /**
     * @throws FailureException
     */
    public function setCharset(string $charset, string $collate = ""): Promise {
        if (! $this->processor) {
            throw new FailureException('Not connected');
        }

        return $this->processor->setCharset($charset, $collate);
    }

    public function close() {
        $processor = $this->processor;
        // Send close command if connection is not already in a closed or closing state
        if ($processor && $processor->isAlive()) {
            $processor->sendClose()->onResolve(static function () use ($processor) {
                $processor->close();
            });
        }
    }

    /**
     * @throws FailureException
     */
    public function useDb(string $db): Promise {
        if (! $this->processor) {
            throw new FailureException('Not connected');
        }

        return $this->processor->useDb($db);
    }

    /**
     * @param int $subcommand int one of the self::REFRESH_* constants
     *
     * @return \Amp\Promise
     *
     * @throws FailureException
     */
    public function refresh(int $subcommand): Promise {
        if (! $this->processor) {
            throw new FailureException('Not connected');
        }

        return $this->processor->refresh($subcommand);
    }

    public function query(string $query): Promise {
        if (! $this->processor) {
            throw new FailureException('Not connected');
        }

        return call(function () use ($query) {
            while ($this->busy) {
                yield $this->busy->promise();
            }

            $result = yield $this->processor->query($query);

            if ($result instanceof Internal\ResultProxy) {
                return new ResultSet($result);
            }

            if ($result instanceof CommandResult) {
                return $result;
            }

            throw new FailureException("Unrecognized result type");
        });
    }

    /**
     * @throws FailureException
     */
    public function transaction(int $isolation = Transaction::COMMITTED): Promise {
        if (! $this->processor) {
            throw new FailureException('Not connected');
        }

        return call(function () use ($isolation) {
            switch ($isolation) {
                case Transaction::UNCOMMITTED:
                    yield $this->query("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
                    break;

                case Transaction::COMMITTED:
                    yield $this->query("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED");
                    break;

                case Transaction::REPEATABLE:
                    yield $this->query("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ");
                    break;

                case Transaction::SERIALIZABLE:
                    yield $this->query("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE");
                    break;

                default:
                    throw new \Error("Invalid transaction type");
            }

            yield $this->query("START TRANSACTION");

            $this->busy = new Deferred;

            $transaction = new Transaction($this->processor, $isolation);
            $transaction->onDestruct(function () {
                \assert($this->busy !== null);

                $deferred = $this->busy;
                $this->busy = null;
                $deferred->resolve();
            });

            return $transaction;
        });
    }

    /**
     * @throws FailureException
     */
    public function ping(): Promise {
        if (! $this->processor) {
            throw new FailureException('Not connected');
        }

        return $this->processor->ping();
    }

    public function prepare(string $query): Promise {
        if (! $this->processor) {
            throw new FailureException('Not connected');
        }

        return call(function () use ($query) {
            while ($this->busy) {
                yield $this->busy->promise();
            }

            return $this->processor->prepare($query);
        });
    }

    /**
     * {@inheritdoc}
     */
    public function execute(string $sql, array $params = []): Promise {
        if (! $this->processor) {
            throw new FailureException('Not connected');
        }

        return call(function () use ($sql, $params) {
            /** @var \Amp\Mysql\Statement $statment */
            $statment = yield $this->prepare($sql);
            return yield $statment->execute($params);
        });
    }

    public function __destruct() {
        if ($this->processor) {
            $this->processor->unreference();
        }
    }
}
