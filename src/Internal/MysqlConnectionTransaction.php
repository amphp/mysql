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
    const SAVEPOINT_PREFIX = "amp_";

    private ?ConnectionProcessor $processor;

    private readonly TransactionIsolation $isolation;

    /** @var \Closure():void */
    private readonly \Closure $release;

    private int $refCount = 1;

    private readonly DeferredFuture $onClose;

    /**
     * @param \Closure():void $release
     *
     * @throws \Error If the isolation level is invalid.
     */
    public function __construct(
        ConnectionProcessor $processor,
        \Closure $release,
        TransactionIsolation $isolation
    ) {
        $this->processor = $processor;
        $this->isolation = $isolation;

        $refCount =& $this->refCount;
        $this->release = static function () use (&$refCount, $release): void {
            if (--$refCount === 0) {
                $release();
            }
        };

        $this->onClose = new DeferredFuture();
        $this->onClose($this->release);
    }

    public function __destruct()
    {
        if ($this->onClose->isComplete()) {
            return;
        }

        $this->onClose->complete();

        if (!$this->processor || $this->processor->isClosed()) {
            return;
        }

        $processor = $this->processor;
        EventLoop::queue(static function () use ($processor): void {
            try {
                !$processor->isClosed() && $processor->query('ROLLBACK');
            } catch (SqlException) {
                // Ignore failure if connection closes during query.
            }
        });
    }

    public function getLastUsedAt(): int
    {
        return $this->processor?->getLastUsedAt() ?? 0;
    }

    /**
     * Closes and rolls back all changes in the transaction.
     */
    public function close(): void
    {
        if ($this->processor) {
            $this->rollback(); // Invokes $this->release callback.
        }
    }

    public function isClosed(): bool
    {
        return !$this->processor || $this->processor->isClosed();
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
        return $this->processor !== null;
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
        if ($this->processor === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        $result = $this->processor->query($sql)->await();
        ++$this->refCount;
        return new MysqlPooledResult($result, $this->release);
    }

    /**
     * @throws TransactionError If the transaction has been committed or rolled back.
     *
     * @psalm-suppress InvalidReturnStatement, InvalidReturnType
     */
    public function prepare(string $sql): MysqlStatement
    {
        if ($this->processor === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        $statement = $this->processor->prepare($sql)->await();
        return new MysqlPooledStatement($statement, $this->release);
    }

    /**
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function execute(string $sql, array $params = []): MysqlResult
    {
        if ($this->processor === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        $statement = $this->processor->prepare($sql)->await();
        $result = $statement->execute($params);

        ++$this->refCount;
        return new MysqlPooledResult($result, $this->release);
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

        $future = $this->processor->query("COMMIT");
        $this->processor = null;
        $future->finally($this->onClose->complete(...))->await();
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

        $future = $this->processor->query("ROLLBACK");
        $this->processor = null;
        $future->finally($this->onClose->complete(...))->await();
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
