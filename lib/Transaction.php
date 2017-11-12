<?php

namespace Amp\Mysql;

use Amp\Promise;

class Transaction implements Executor {
    const UNCOMMITTED  = 0;
    const COMMITTED    = 1;
    const REPEATABLE   = 2;
    const SERIALIZABLE = 4;

    /** @var \Amp\Mysql\Connection */
    private $connection;

    /** @var \Amp\Mysql\Internal\CompletionQueue */
    private $queue;

    /**
     * @param \Amp\Mysql\Connection $connection
     * @param int $isolation
     *
     * @throws \Error If the isolation level is invalid.
     */
    public function __construct(Connection $connection) {
        $this->connection = $connection;
        $this->queue = new Internal\CompletionQueue;
    }

    public function __destruct() {
        if ($this->connection) {
            $this->rollback(); // Invokes $this->queue->complete().
        }
    }

    public function close() {
        if ($this->connection) {
            $this->commit(); // Invokes $this->queue->complete().
        }
    }

    /**
     * {@inheritdoc}
     */
    public function onComplete(callable $onComplete) {
        $this->queue->onComplete($onComplete);
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

        return $this->connection->query($sql);
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

        return $this->connection->prepare($sql);
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
        $promise->onResolve([$this->queue, "complete"]);

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
        $promise->onResolve([$this->queue, "complete"]);

        return $promise;
    }

    /**
     * Creates a savepoint with the given identifier. WARNING: Identifier is not sanitized, do not pass untrusted data.
     *
     * @param string $identifier Savepoint identifier.
     *
     * @return \Amp\Promise<\Amp\Mysql\CommandResult>
     *
     * @throws \Amp\Mysql\TransactionError If the transaction has been committed or rolled back.
     */
    public function savepoint(string $identifier): Promise {
        return $this->query("SAVEPOINT " . $identifier);
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
        return $this->query("ROLLBACK TO " . $identifier);
    }

    /**
     * Releases the savepoint with the given identifier. WARNING: Identifier is not sanitized, do not pass untrusted
     * data.
     *
     * @param string $identifier Savepoint identifier.
     *
     * @return \Amp\Promise<\Amp\Mysql\CommandResult>
     *
     * @throws \Amp\Mysql\TransactionError If the transaction has been committed or rolled back.
     */
    public function release(string $identifier): Promise {
        return $this->query("RELEASE SAVEPOINT " . $identifier);
    }
}
