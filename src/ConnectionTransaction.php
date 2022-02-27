<?php

namespace Amp\Mysql;

use Amp\Promise;
use Amp\Sql\Transaction as SqlTransaction;
use Amp\Sql\TransactionError;
use function Amp\call;

final class ConnectionTransaction implements SqlTransaction
{
    const SAVEPOINT_PREFIX = "amp_";

    /** @var Internal\Processor|null */
    private $processor;

    /** @var int */
    private $isolation;

    /** @var callable */
    private $release;

    /** @var int */
    private $refCount = 1;

    /**
     * @param Internal\Processor $processor
     * @param callable $release
     * @param int $isolation
     *
     * @throws \Error If the isolation level is invalid.
     */
    public function __construct(Internal\Processor $processor, callable $release, int $isolation = SqlTransaction::ISOLATION_COMMITTED)
    {
        switch ($isolation) {
            case SqlTransaction::ISOLATION_UNCOMMITTED:
            case SqlTransaction::ISOLATION_COMMITTED:
            case SqlTransaction::ISOLATION_REPEATABLE:
            case SqlTransaction::ISOLATION_SERIALIZABLE:
                $this->isolation = $isolation;
                break;

            default:
                throw new \Error("Isolation must be a valid transaction isolation level");
        }

        $this->processor = $processor;

        $refCount =& $this->refCount;
        $this->release = static function () use (&$refCount, $release) {
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
    public function query(string $sql): Promise
    {
        if ($this->processor === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return call(function () use ($sql) {
            $result = yield $this->processor->query($sql);

            if ($result instanceof Internal\ResultProxy) {
                ++$this->refCount;
                return new PooledResultSet(new ConnectionResultSet($result), $this->release);
            }

            return $result;
        });
    }

    /**
     * {@inheritdoc}
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function prepare(string $sql): Promise
    {
        if ($this->processor === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return call(function () use ($sql) {
            $statement = yield $this->processor->prepare($sql);
            return new PooledStatement($statement, $this->release);
        });
    }

    /**
     * {@inheritdoc}
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function execute(string $sql, array $params = []): Promise
    {
        if ($this->processor === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return call(function () use ($sql, $params) {
            $statement = yield $this->processor->prepare($sql);
            \assert($statement instanceof Statement);
            $result = yield $statement->execute($params);

            if ($result instanceof ResultSet) {
                ++$this->refCount;
                return new PooledResultSet($result, $this->release);
            }

            return $result;
        });
    }

    /**
     * Commits the transaction and makes it inactive.
     *
     * @return Promise<\Amp\Sql\CommandResult>
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function commit(): Promise
    {
        if ($this->processor === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        $promise = $this->processor->query("COMMIT");
        $this->processor = null;
        $promise->onResolve($this->release);

        return $promise;
    }

    /**
     * Rolls back the transaction and makes it inactive.
     *
     * @return Promise<\Amp\Sql\CommandResult>
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function rollback(): Promise
    {
        if ($this->processor === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        $promise = $this->processor->query("ROLLBACK");
        $this->processor = null;
        $promise->onResolve($this->release);

        return $promise;
    }

    /**
     * Creates a savepoint with the given identifier.
     *
     * @param string $identifier Savepoint identifier.
     *
     * @return Promise<\Amp\Sql\CommandResult>
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function createSavepoint(string $identifier): Promise
    {
        return $this->query(\sprintf("SAVEPOINT `%s%s`", self::SAVEPOINT_PREFIX, \sha1($identifier)));
    }

    /**
     * Rolls back to the savepoint with the given identifier.
     *
     * @param string $identifier Savepoint identifier.
     *
     * @return Promise<\Amp\Sql\CommandResult>
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function rollbackTo(string $identifier): Promise
    {
        return $this->query(\sprintf("ROLLBACK TO `%s%s`", self::SAVEPOINT_PREFIX, \sha1($identifier)));
    }

    /**
     * Releases the savepoint with the given identifier.
     *
     * @param string $identifier Savepoint identifier.
     *
     * @return Promise<\Amp\Sql\CommandResult>
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function releaseSavepoint(string $identifier): Promise
    {
        return $this->query(\sprintf("RELEASE SAVEPOINT `%s%s`", self::SAVEPOINT_PREFIX, \sha1($identifier)));
    }
}
