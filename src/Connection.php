<?php

namespace Amp\Mysql;

use Amp\CancellationToken;
use Amp\Deferred;
use Amp\NullCancellationToken;
use Amp\Socket;
use Revolt\EventLoop;

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
            $busy->complete(null);
            $busy = null;
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
        $this->processor->setCharset($charset, $collate)->await();
    }

    public function close(): void
    {
        $processor = $this->processor;
        // Send close command if connection is not already in a closed or closing state
        if ($processor->isAlive()) {
            $processor->sendClose()->finally(static fn() => $processor->close())->ignore();
        }
    }

    public function useDb(string $db): void
    {
        $this->processor->useDb($db)->await();
    }

    /**
     * @param int $subcommand int one of the self::REFRESH_* constants
     */
    public function refresh(int $subcommand): void
    {
        $this->processor->refresh($subcommand)->await();
    }

    public function query(string $sql): Result
    {
        while ($this->busy) {
            $this->busy->getFuture()->await();
        }

        return $this->processor->query($sql)->await();
    }

    public function beginTransaction(int $isolation = Transaction::ISOLATION_COMMITTED): Transaction
    {
        while ($this->busy) {
            $this->busy->getFuture()->await();
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
            $promise->await();
            $this->processor->query("START TRANSACTION")->await();
        } catch (\Throwable $exception) {
            $this->busy = null;
            $deferred->complete(null);
            throw $exception;
        }

        return new Internal\ConnectionTransaction($this->processor, $this->release, $isolation);
    }

    public function ping(): void
    {
        $this->processor->ping()->await();
    }

    public function prepare(string $sql): Statement
    {
        while ($this->busy) {
            $this->busy->getFuture()->await();
        }

        return $this->processor->prepare($sql)->await();
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
        $processor = $this->processor;
        EventLoop::queue(static fn () => $processor->unreference());
    }
}
