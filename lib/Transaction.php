<?php

namespace Amp\Mysql;

use Amp\Promise;
use function Amp\call;

class Transaction implements Executor, Operation {
    const UNCOMMITTED  = 0;
    const COMMITTED    = 1;
    const REPEATABLE   = 2;
    const SERIALIZABLE = 4;

    const SAVEPOINT_PREFIX = "amp_";

    /** @var \Amp\Mysql\Internal\Processor */
    private $processor;

    /** @var \Amp\Mysql\Internal\ReferenceQueue */
    private $queue;

    /** @var int */
    private $isolation;

    /**
     * @param \Amp\Mysql\Internal\Processor $processor
     * @param int $isolation
     *
     * @throws \Error If the isolation level is invalid.
     */
    public function __construct(Internal\Processor $processor, int $isolation) {
        $this->processor = $processor;
        $this->isolation = $isolation;
        $this->queue = new Internal\ReferenceQueue;
    }

    public function __destruct() {
        if ($this->processor) {
            $this->rollback(); // Invokes $this->queue->unreference().
        }
    }

    /**
     * {@inheritdoc}
     *
     * Closes and commits all changes in the transaction.
     */
    public function close() {
        if ($this->processor) {
            $this->commit(); // Invokes $this->queue->unreference().
        }
    }

    /**
     * @return int
     */
    public function getIsolationLevel(): int {
        return $this->isolation;
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
    public function isAlive(): bool {
        return $this->processor !== null && $this->processor->isAlive();
    }

    /**
     * @return bool True if the transaction is active, false if it has been committed or rolled back.
     */
    public function isActive(): bool {
        return $this->processor !== null;
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Amp\Mysql\TransactionError If the transaction has been committed or rolled back.
     */
    public function query(string $sql): Promise {
        if ($this->processor === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return call(function () use ($sql) {
            $this->queue->reference();

            try {
                $result = yield $this->processor->query($sql);
            } catch (\Throwable $exception) {
                $this->queue->unreference();
                throw $exception;
            }

            if ($result instanceof Internal\ResultProxy) {
                $result = new ResultSet($result);
                $result->onDestruct([$this->queue, "unreference"]);
            } else {
                $this->queue->unreference();
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
        if ($this->processor === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        $this->queue->reference();

        $promise = $this->processor->prepare($sql);

        $promise->onResolve(function ($exception, $statement) {
            if ($statement instanceof Statement) {
                $statement->onDestruct([$this->queue, "unreference"]);
                return;
            }

            $this->queue->unreference();
        });

        return $promise;
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Amp\Mysql\TransactionError If the transaction has been committed or rolled back.
     */
    public function execute(string $sql, array $params = []): Promise {
        if ($this->processor === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return call(function () use ($sql, $params) {
            $this->queue->reference();

            try {
                /** @var \Amp\Mysql\Statement $statement */
                $statement = yield $this->processor->prepare($sql);
                $result = yield $statement->execute($params);
            } catch (\Throwable $exception) {
                $this->queue->unreference();
                throw $exception;
            }

            if ($result instanceof ResultSet) {
                $result->onDestruct([$this->queue, "unreference"]);
            } else {
                $this->queue->unreference();
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
        if ($this->processor === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        $promise = $this->processor->query("COMMIT");
        $this->processor = null;
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
        if ($this->processor === null) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        $promise = $this->processor->query("ROLLBACK");
        $this->processor = null;
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
        return $this->query(\sprintf("ROLLBACK TO `%s%s`", self::SAVEPOINT_PREFIX, \sha1($identifier)));
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
        return $this->query(\sprintf("RELEASE SAVEPOINT `%s%s`", self::SAVEPOINT_PREFIX, \sha1($identifier)));
    }
}
