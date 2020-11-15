<?php

namespace Amp\Mysql;

use Amp\CancellationToken;
use Amp\Deferred;
use Amp\NullCancellationToken;
use Amp\Socket;
use function Amp\await;

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

    private Internal\Processor $processor;

    private ?Deferred $busy = null;

    /** @var \Closure Function used to release connection after a transaction has completed. */
    private \Closure $release;

    /**
     * @param ConnectionConfig $config
     * @param CancellationToken|null $token
     * @param Socket\Connector|null $connector
     *
     * @return self
     */
    public static function connect(
        ConnectionConfig $config,
        ?CancellationToken $token = null,
        ?Socket\Connector $connector = null
    ): self {
        $token = $token ?? new NullCancellationToken;

        $socket = ($connector ?? Socket\connector())
            ->connect($config->getConnectionString(), $config->getConnectContext(), $token);

        $processor = new Internal\Processor($socket, $config);
        $processor->connect($token);
        return new self($processor);
    }

    /**
     * @param Internal\Processor $processor
     */
    private function __construct(Internal\Processor $processor)
    {
        $this->processor = $processor;

        $busy = &$this->busy;
        $this->release = static function () use (&$busy): void {
            \assert($busy instanceof Deferred);
            $deferred = $busy;
            $busy = null;
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

    public function setCharset(string $charset, string $collate = ""): void
    {
        await($this->processor->setCharset($charset, $collate));
    }

    public function close(): void
    {
        $processor = $this->processor;
        // Send close command if connection is not already in a closed or closing state
        if ($processor->isAlive()) {
            $processor->sendClose()->onResolve(static fn() => $processor->close());
        }
    }

    public function useDb(string $db): void
    {
        await($this->processor->useDb($db));
    }

    /**
     * @param int $subcommand int one of the self::REFRESH_* constants
     */
    public function refresh(int $subcommand): void
    {
        $this->processor->refresh($subcommand);
    }

    public function query(string $query): Result
    {
        while ($this->busy) {
            await($this->busy->promise());
        }

        return await($this->processor->query($query));
    }

    public function beginTransaction(int $isolation = Transaction::ISOLATION_COMMITTED): Transaction
    {
        while ($this->busy) {
            await($this->busy->promise());
        }

        $this->busy = $deferred = new Deferred;

        switch ($isolation) {
            case Transaction::ISOLATION_UNCOMMITTED:
                $promise = $this->processor->query("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
                break;

            case Transaction::ISOLATION_COMMITTED:
                $promise = $this->processor->query("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED");
                break;

            case Transaction::ISOLATION_REPEATABLE:
                $promise = $this->processor->query("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ");
                break;

            case Transaction::ISOLATION_SERIALIZABLE:
                $promise = $this->processor->query("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE");
                break;

            default:
                throw new \Error("Invalid transaction type");
        }

        try {
            await($promise);
            await($this->processor->query("START TRANSACTION"));
        } catch (\Throwable $exception) {
            $this->busy = null;
            $deferred->resolve();
            throw $exception;
        }

        return new Internal\ConnectionTransaction($this->processor, $this->release, $isolation);
    }

    public function ping(): void
    {
        await($this->processor->ping());
    }

    public function prepare(string $query): Statement
    {
        while ($this->busy) {
            await($this->busy->promise());
        }

        return await($this->processor->prepare($query));
    }

    /**
     * {@inheritdoc}
     */
    public function execute(string $sql, array $params = []): Result
    {
        $statement = $this->prepare($sql);
        return $statement->execute($params);
    }

    public function __destruct()
    {
        $this->processor->unreference();
    }
}
