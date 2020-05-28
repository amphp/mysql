<?php

namespace Amp\Mysql;

use Amp\CancellationToken;
use Amp\Deferred;
use Amp\NullCancellationToken;
use Amp\Promise;
use Amp\Socket;
use Amp\Sql\Link;
use Amp\Sql\Transaction;
use function Amp\call;

final class Connection implements Link
{
    public const REFRESH_GRANT = 0x01;
    public const REFRESH_LOG = 0x02;
    public const REFRESH_TABLES = 0x04;
    public const REFRESH_HOSTS = 0x08;
    public const REFRESH_STATUS = 0x10;
    public const REFRESH_THREADS = 0x20;
    public const REFRESH_SLAVE = 0x40;
    public const REFRESH_MASTER = 0x80;

    /** @var Internal\Processor */
    private $processor;

    /** @var Deferred|null */
    private $busy;

    /** @var callable Function used to release connection after a transaction has completed. */
    private $release;

    /**
     * @param ConnectionConfig $config
     * @param CancellationToken|null $token
     * @param Socket\Connector|null $connector
     *
     * @return Promise<self>
     */
    public static function connect(
        ConnectionConfig $config,
        ?CancellationToken $token = null,
        ?Socket\Connector $connector = null
    ): Promise {
        $token = $token ?? new NullCancellationToken;

        return call(function () use ($config, $token, $connector) {
            $socket = yield ($connector ?? Socket\connector())
                ->connect($config->getConnectionString(), $config->getConnectContext(), $token);

            $processor = new Internal\Processor($socket, $config);
            yield $processor->connect($token);
            return new self($processor);
        });
    }

    /**
     * @param Internal\Processor $processor
     */
    private function __construct(Internal\Processor $processor)
    {
        $this->processor = $processor;
        $this->release = function () {
            \assert($this->busy instanceof Deferred);
            $deferred = $this->busy;
            $this->busy = null;
            $deferred->resolve();
        };
    }

    /**
     * @return bool False if the connection has been closed.
     */
    public function isAlive(): bool
    {
        return $this->processor->isAlive();
    }

    /**
     * @return int Timestamp of the last time this connection was used.
     */
    public function getLastUsedAt(): int
    {
        return $this->processor->getLastUsedAt();
    }

    public function isReady(): bool
    {
        return $this->processor->isReady();
    }

    public function setCharset(string $charset, string $collate = ""): Promise
    {
        return $this->processor->setCharset($charset, $collate);
    }

    public function close(): void
    {
        $processor = $this->processor;
        // Send close command if connection is not already in a closed or closing state
        if ($processor->isAlive()) {
            $processor->sendClose()->onResolve(static function () use ($processor) {
                $processor->close();
            });
        }
    }

    public function useDb(string $db): Promise
    {
        return $this->processor->useDb($db);
    }

    /**
     * @param int $subcommand int one of the self::REFRESH_* constants
     *
     * @return Promise
     */
    public function refresh(int $subcommand): Promise
    {
        return $this->processor->refresh($subcommand);
    }

    public function query(string $query): Promise
    {
        return call(function () use ($query) {
            while ($this->busy) {
                yield $this->busy->promise();
            }

            $result = yield $this->processor->query($query);

            \assert($result instanceof Result);

            return $result;
        });
    }

    public function beginTransaction(int $isolation = Transaction::ISOLATION_COMMITTED): Promise
    {
        return call(function () use ($isolation) {
            switch ($isolation) {
                case Transaction::ISOLATION_UNCOMMITTED:
                    yield $this->query("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
                    break;

                case Transaction::ISOLATION_COMMITTED:
                    yield $this->query("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED");
                    break;

                case Transaction::ISOLATION_REPEATABLE:
                    yield $this->query("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ");
                    break;

                case Transaction::ISOLATION_SERIALIZABLE:
                    yield $this->query("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE");
                    break;

                default:
                    throw new \Error("Invalid transaction type");
            }

            yield $this->query("START TRANSACTION");

            $this->busy = new Deferred;

            return new Internal\ConnectionTransaction($this->processor, $this->release, $isolation);
        });
    }

    public function ping(): Promise
    {
        return $this->processor->ping();
    }

    public function prepare(string $query): Promise
    {
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
    public function execute(string $sql, array $params = []): Promise
    {
        return call(function () use ($sql, $params) {
            $statement = yield $this->prepare($sql);
            \assert($statement instanceof Statement);
            return yield $statement->execute($params);
        });
    }

    public function __destruct()
    {
        $this->processor->unreference();
    }
}
