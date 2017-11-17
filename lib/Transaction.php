<?php

namespace Amp\Mysql;

use Amp\Promise;
use function Amp\call;

class Transaction implements Executor, Operation {
    const UNCOMMITTED  = 0;
    const COMMITTED    = 1;
    const REPEATABLE   = 2;
    const SERIALIZABLE = 4;

    /** @var \Amp\Mysql\Connection */
    private $connection;

    /** @var \Amp\Mysql\Internal\ReferenceQueue */
    private $queue;

    /**
     * @param \Amp\Mysql\Connection $connection
     * @param int $isolation
     *
     * @throws \Error If the isolation level is invalid.
     */
    public function __construct(Connection $connection) {
        $this->connection = $connection;
        $this->queue = new Internal\ReferenceQueue;
    }

    public function __destruct() {
        if ($this->connection) {
            $this->rollback(); // Invokes $this->queue->unreference().
        }
    }

    /**
     * {@inheritdoc}
     *
     * Closes and commits all changes in the transaction.
     */
    public function close() {
        if ($this->connection) {
            $this->commit(); // Invokes $this->queue->unreference().
        }
    }

    /**
     * {@inheritdoc}
     */
    public function onDestruct(callable $onDestruct) {
        $this->queue->onDestruct($onDestruct);
    }

    /**
     * {@inheritdoc}
     */
    public function alive(): bool {
        return $this->connection !== null && $this->connection->isAlive();
    }

    /**
     * @return bool True if the transaction is active, false if it has been committed or rolled back.
     */
    public function isActive(): bool {
        return $this->connection !== null;
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Amp\Mysql\TransactionError If the transaction has been committed or rolled back.
     */
    public function query(string $sql): Promise {
        if ($this->connection === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return call(function () use ($sql) {
            $result = yield $this->connection->query($sql);

            if ($result instanceof Operation) {
                $this->queue->reference();
                $result->onDestruct([$this->queue, "unreference"]);
            }

            return $result;
        });
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Amp\Mysql\TransactionError If the transaction has been committed or rolled back.
     */
    public function prepare(string $sql): Promise {
        if ($this->connection === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return call(function () use ($sql) {
            /** @var \Amp\Mysql\Statement $statement */
            $statement = yield $this->connection->prepare($sql);
            $this->queue->reference();
            $statement->onDestruct([$this->queue, "unreference"]);
            return $statement;
        });
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Amp\Mysql\TransactionError If the transaction has been committed or rolled back.
     */
    public function execute(string $sql, array $params = []): Promise {
        if ($this->connection === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return call(function () use ($sql, $params) {
            $result = yield $this->connection->execute($sql, $params);

            if ($result instanceof Operation) {
                $this->queue->reference();
                $result->onDestruct([$this->queue, "unreference"]);
            }

            return $result;
        });
    }

    /**
     * Commits the transaction and makes it inactive.
     *
     * @return \Amp\Promise<\Amp\Mysql\CommandResult>
     *
     * @throws \Amp\Mysql\TransactionError If the transaction has been committed or rolled back.
     */
    public function commit(): Promise {
        if ($this->connection === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        $promise = $this->connection->query("COMMIT");
        $this->connection = null;
        $promise->onResolve([$this->queue, "unreference"]);

        return $promise;
    }

    /**
     * Rolls back the transaction and makes it inactive.
     *
     * @return \Amp\Promise<\Amp\Mysql\CommandResult>
     *
     * @throws \Amp\Mysql\TransactionError If the transaction has been committed or rolled back.
     */
    public function rollback(): Promise {
        if ($this->connection === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        $promise = $this->connection->query("ROLLBACK");
        $this->connection = null;
        $promise->onResolve([$this->queue, "unreference"]);

        return $promise;
    }

    /**
     * Creates a savepoint with the given identifier.
     *
     * @param string $identifier Savepoint identifier.
     *
     * @return \Amp\Promise<\Amp\Mysql\CommandResult>
     *
     * @throws \Amp\Mysql\TransactionError If the transaction has been committed or rolled back.
     */
    public function savepoint(string $identifier): Promise {
        return $this->query(\sprintf("SAVEPOINT `%s%s`", self::SAVEPOINT_PREFIX, \sha1($identifier)));
    }

    /**
     * Rolls back to the savepoint with the given identifier.
     *
     * @param string $identifier Savepoint identifier.
     *
     * @return \Amp\Promise<\Amp\Mysql\CommandResult>
     *
     * @throws \Amp\Mysql\TransactionError If the transaction has been committed or rolled back.
     */
    public function rollbackTo(string $identifier): Promise {
        return $this->query("ROLLBACK TO " . \sprintf("SAVEPOINT `%s%s`", self::SAVEPOINT_PREFIX, \sha1($identifier)));
    }

    /**
     * Releases the savepoint with the given identifier.
     *
     * @param string $identifier Savepoint identifier.
     *
     * @return \Amp\Promise<\Amp\Mysql\CommandResult>
     *
     * @throws \Amp\Mysql\TransactionError If the transaction has been committed or rolled back.
     */
    public function release(string $identifier): Promise {
        return $this->query("RELEASE SAVEPOINT " . \sprintf("SAVEPOINT `%s%s`", self::SAVEPOINT_PREFIX, \sha1($identifier)));
    }
}
