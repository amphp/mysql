<?php declare(strict_types=1);

namespace Amp\Mysql\Internal;

use Amp\DeferredFuture;
use Amp\Mysql\MysqlResult;
use Amp\Mysql\MysqlStatement;
use Amp\Mysql\MysqlTransaction;
use Amp\Sql\SqlException;
use Amp\Sql\TransactionError;
use Amp\Sql\TransactionIsolation;
use Revolt\EventLoop;

/** @internal */
final class MysqlConnectionTransaction implements MysqlTransaction
{
    /** @var \Closure():void */
    private readonly \Closure $release;

    private int $refCount = 1;

    private bool $active = true;

    private readonly DeferredFuture $onCommit;
    private readonly DeferredFuture $onRollback;
    private readonly DeferredFuture $onClose;

    private ?DeferredFuture $busy = null;

    /**
     * @param \Closure():void $release
     *
     * @throws \Error If the isolation level is invalid.
     */
    public function __construct(
        private readonly MysqlNestableExecutor $executor,
        \Closure $release,
        private readonly TransactionIsolation $isolation,
    ) {
        $busy = &$this->busy;
        $refCount = &$this->refCount;
        $this->release = static function () use (&$busy, &$refCount, $release): void {
            $busy?->complete();
            $busy = null;

            if (--$refCount === 0) {
                $release();
            }
        };

        $this->onCommit = new DeferredFuture();
        $this->onRollback = new DeferredFuture();
        $this->onClose = new DeferredFuture();

        $this->onClose($this->release);
    }

    public function __destruct()
    {
        if (!$this->isActive()) {
            return;
        }

        $busy = &$this->busy;
        $executor = $this->executor;
        $onRollback = $this->onRollback;
        $onClose = $this->onClose;
        EventLoop::queue(static function () use (&$busy, $executor, $onRollback, $onClose): void {
            try {
                while ($busy) {
                    $busy->getFuture()->await();
                }

                if (!$executor->isClosed()) {
                    $executor->query('ROLLBACK');
                }
            } catch (SqlException) {
                // Ignore failure if connection closes during query.
            } finally {
                $onRollback->complete();
                $onClose->complete();
            }
        });
    }

    public function getLastUsedAt(): int
    {
        return $this->executor->getLastUsedAt();
    }

    public function isNestedTransaction(): bool
    {
        return false;
    }

    /**
     * Closes and rolls back all changes in the transaction.
     */
    public function close(): void
    {
        if ($this->isActive()) {
            $this->rollback(); // Invokes $this->release callback.
        }
    }

    public function isClosed(): bool
    {
        return !$this->isActive();
    }

    public function onClose(\Closure $onClose): void
    {
        $this->onClose->getFuture()->finally($onClose);
    }

    /**
     * @return bool True if the transaction is active, false if it has been committed or rolled back.
     */
    public function isActive(): bool
    {
        return $this->active;
    }

    public function getIsolationLevel(): TransactionIsolation
    {
        return $this->isolation;
    }

    /**
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function query(string $sql): MysqlResult
    {
        $this->awaitPendingNestedTransaction();

        ++$this->refCount;
        try {
            $result = $this->executor->query($sql);
        } catch (\Throwable $exception) {
            EventLoop::queue($this->release);
            throw $exception;
        }

        return new MysqlPooledResult($result, $this->release);
    }

    /**
     * @throws TransactionError If the transaction has been committed or rolled back.
     *
     * @psalm-suppress InvalidReturnStatement, InvalidReturnType
     */
    public function prepare(string $sql): MysqlStatement
    {
        $this->awaitPendingNestedTransaction();

        ++$this->refCount;
        try {
            $statement = $this->executor->prepare($sql);
        } catch (\Throwable $exception) {
            EventLoop::queue($this->release);
            throw $exception;
        }

        return new MysqlPooledStatement($statement, $this->release);
    }

    /**
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function execute(string $sql, array $params = []): MysqlResult
    {
        $this->awaitPendingNestedTransaction();

        ++$this->refCount;
        try {
            $statement = $this->executor->prepare($sql);
            $result = $statement->execute($params);
        } catch (\Throwable $exception) {
            EventLoop::queue($this->release);
            throw $exception;
        }

        return new MysqlPooledResult($result, $this->release);
    }

    public function beginTransaction(): MysqlTransaction
    {
        $this->awaitPendingNestedTransaction();

        ++$this->refCount;
        $this->busy = new DeferredFuture();
        try {
            $identifier = \bin2hex(\random_bytes(8));
            $this->executor->createSavepoint($identifier);
        } catch (\Throwable $exception) {
            EventLoop::queue($this->release);
            throw $exception;
        }

        return new MysqlNestedTransaction($this, $this->executor, $identifier, $this->release);
    }

    /**
     * Commits the transaction and makes it inactive.
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function commit(): void
    {
        $this->awaitPendingNestedTransaction();

        $this->active = false;
        $this->executor->query("COMMIT");

        $this->onCommit->complete();
        $this->onClose->complete();
    }

    /**
     * Rolls back the transaction and makes it inactive.
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function rollback(): void
    {
        $this->awaitPendingNestedTransaction();

        $this->active = false;
        $this->executor->query("ROLLBACK");

        $this->onRollback->complete();
        $this->onClose->complete();
    }

    public function onCommit(\Closure $onCommit): void
    {
        $this->onCommit->getFuture()->finally($onCommit);
    }

    public function onRollback(\Closure $onRollback): void
    {
        $this->onRollback->getFuture()->finally($onRollback);
    }

    private function assertOpen(): void
    {
        if ($this->isClosed()) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }
    }

    private function awaitPendingNestedTransaction(): void
    {
        while ($this->busy) {
            $this->busy->getFuture()->await();
        }

        $this->assertOpen();
    }
}
