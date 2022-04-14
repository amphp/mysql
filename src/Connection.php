<?php

namespace Amp\Mysql;

use Amp\Cancellation;
use Amp\DeferredFuture;
use Amp\Socket\Socket;
use Amp\Sql\TransactionIsolation;
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

    private ?DeferredFuture $busy = null;

    /** @var \Closure Function used to release connection after a transaction has completed. */
    private \Closure $release;

    public static function initialize(
        Socket $socket,
        MysqlConfig $config,
        ?Cancellation $cancellation = null,
    ): self {
        $processor = new Internal\Processor($socket, $config);
        $processor->connect($cancellation);
        return new self($processor);
    }

    private function __construct(Internal\Processor $processor)
    {
        $this->processor = $processor;

        $busy = &$this->busy;
        $this->release = static function () use (&$busy): void {
            \assert($busy instanceof DeferredFuture);
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
        // Send close command if connection is not already in a closed or closing state
        if ($this->processor->isAlive()) {
            $this->processor->sendClose()->await();
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

    public function beginTransaction(TransactionIsolation $isolation = TransactionIsolation::Committed): Transaction
    {
        while ($this->busy) {
            $this->busy->getFuture()->await();
        }

        $this->busy = $deferred = new DeferredFuture;

        try {
            $this->processor->query("SET SESSION TRANSACTION ISOLATION LEVEL " . $isolation->toSql())->await();
            $this->processor->query("START TRANSACTION")->await();
        } catch (\Throwable $exception) {
            $this->busy = null;
            $deferred->complete();
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
