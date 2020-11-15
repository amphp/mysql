<?php

namespace Amp\Mysql\Internal;

use Amp\Mysql\PooledResult;
use Amp\Mysql\PooledStatement;
use Amp\Mysql\Result;
use Amp\Mysql\Statement;
use Amp\Mysql\Transaction;
use Amp\Sql\TransactionError;
use function Amp\await;

final class ConnectionTransaction implements Transaction
{
    const SAVEPOINT_PREFIX = "amp_";

    private ?Processor $processor;

    private int $isolation;

    /** @var callable */
    private $release;

    private int $refCount = 1;

    /**
     * @param Processor $processor
     * @param callable $release
     * @param int $isolation
     *
     * @throws \Error If the isolation level is invalid.
     */
    public function __construct(Processor $processor, callable $release, int $isolation = Transaction::ISOLATION_COMMITTED)
    {
        switch ($isolation) {
            case Transaction::ISOLATION_UNCOMMITTED:
            case Transaction::ISOLATION_COMMITTED:
            case Transaction::ISOLATION_REPEATABLE:
            case Transaction::ISOLATION_SERIALIZABLE:
                $this->isolation = $isolation;
                break;

            default:
                throw new \Error("Isolation must be a valid transaction isolation level");
        }

        $this->processor = $processor;

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
            $this->rollback(); // Invokes $this->release callback.
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
            $this->commit(); // Invokes $this->release callback.
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

    /**
     * @return int
     */
    public function getIsolationLevel(): int
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

        $result = await($this->processor->query($sql));
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

        $statement = await($this->processor->prepare($sql));
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

        $statement = await($this->processor->prepare($sql));
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
        $promise->onResolve($this->release);

        await($promise);
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
        $promise->onResolve($this->release);

        await($promise);
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
