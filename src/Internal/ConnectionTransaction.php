<?php

namespace Amp\Mysql\Internal;

use Amp\Mysql\PooledResult;
use Amp\Mysql\PooledStatement;
use Amp\Mysql\Result;
use Amp\Mysql\Statement;
use Amp\Mysql\Transaction;
use Amp\Sql\TransactionError;
use Amp\Sql\TransactionIsolation;
use Revolt\EventLoop;

final class ConnectionTransaction implements Transaction
{
    const SAVEPOINT_PREFIX = "amp_";

    private ?Processor $processor;

    private readonly TransactionIsolation $isolation;

    private readonly \Closure $release;

    private int $refCount = 1;

    /**
     * @param \Closure():void $release
     *
     * @throws \Error If the isolation level is invalid.
     */
    public function __construct(
        Processor $processor,
        \Closure $release,
        TransactionIsolation $isolation = TransactionIsolation::COMMITTED
    ) {
        $this->processor = $processor;
        $this->isolation = $isolation;

        $refCount =& $this->refCount;
        $this->release = static function () use (&$refCount, $release): void {
            if (--$refCount === 0) {
                $release();
            }
        };
    }

    public function __destruct()
    {
        if ($this->processor && $this->processor->isAlive()) {
            $processor = $this->processor;
            $release = $this->release;
            EventLoop::queue(static function () use ($processor, $release): void {
                $processor->query("ROLLBACK");
                $release();
            });
        }
    }

    /**
     * {@inheritdoc}
     */
    public function getLastUsedAt(): int
    {
        return $this->processor->getLastUsedAt();
    }

    /**
     * {@inheritdoc}
     *
     * Closes and commits all changes in the transaction.
     */
    public function close(): void
    {
        if ($this->processor) {
            $this->rollback(); // Invokes $this->release callback.
        }
    }

    /**
     * {@inheritdoc}
     */
    public function isAlive(): bool
    {
        return $this->processor && $this->processor->isAlive();
    }

    /**
     * @return bool True if the transaction is active, false if it has been committed or rolled back.
     */
    public function isActive(): bool
    {
        return $this->processor !== null;
    }

    public function getIsolationLevel(): TransactionIsolation
    {
        return $this->isolation;
    }

    /**
     * {@inheritdoc}
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function query(string $sql): Result
    {
        if ($this->processor === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        $result = $this->processor->query($sql)->await();
        ++$this->refCount;
        return new PooledResult($result, $this->release);
    }

    /**
     * {@inheritdoc}
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function prepare(string $sql): Statement
    {
        if ($this->processor === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        $statement = $this->processor->prepare($sql)->await();
        return new PooledStatement($statement, $this->release);
    }

    /**
     * {@inheritdoc}
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function execute(string $sql, array $params = []): Result
    {
        if ($this->processor === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        $statement = $this->processor->prepare($sql)->await();
        $result = $statement->execute($params);

        ++$this->refCount;
        return new PooledResult($result, $this->release);
    }

    /**
     * Commits the transaction and makes it inactive.
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function commit(): void
    {
        if ($this->processor === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        $promise = $this->processor->query("COMMIT");
        $this->processor = null;
        $promise->finally($this->release)->await();
    }

    /**
     * Rolls back the transaction and makes it inactive.
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function rollback(): void
    {
        if ($this->processor === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        $promise = $this->processor->query("ROLLBACK");
        $this->processor = null;
        $promise->finally($this->release)->await();
    }

    /**
     * Creates a savepoint with the given identifier.
     *
     * @param string $identifier Savepoint identifier.
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function createSavepoint(string $identifier): void
    {
        $this->query(\sprintf("SAVEPOINT `%s%s`", self::SAVEPOINT_PREFIX, \sha1($identifier)));
    }

    /**
     * Rolls back to the savepoint with the given identifier.
     *
     * @param string $identifier Savepoint identifier.
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function rollbackTo(string $identifier): void
    {
        $this->query(\sprintf("ROLLBACK TO `%s%s`", self::SAVEPOINT_PREFIX, \sha1($identifier)));
    }

    /**
     * Releases the savepoint with the given identifier.
     *
     * @param string $identifier Savepoint identifier.
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function releaseSavepoint(string $identifier): void
    {
        $this->query(\sprintf("RELEASE SAVEPOINT `%s%s`", self::SAVEPOINT_PREFIX, \sha1($identifier)));
    }
}
