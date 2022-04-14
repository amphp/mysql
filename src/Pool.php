<?php

namespace Amp\Mysql;

use Amp\Sql\Common\ConnectionPool;
use Amp\Sql\Pool as SqlPool;
use Amp\Sql\Result as SqlResult;
use Amp\Sql\Statement as SqlStatement;
use Amp\Sql\Transaction as SqlTransaction;
use Amp\Sql\TransactionIsolation;

final class Pool extends ConnectionPool implements Link
{
    public function __construct(
        MysqlConfig $config,
        int $maxConnections = self::DEFAULT_MAX_CONNECTIONS,
        int $idleTimeout = self::DEFAULT_IDLE_TIMEOUT,
        ?MysqlConnector $connector = null,
    ) {
        parent::__construct($config, $connector ?? connector(), $maxConnections, $idleTimeout);
    }

    protected function createResult(SqlResult $result, \Closure $release): Result
    {
        \assert($result instanceof Result);
        return new PooledResult($result, $release);
    }

    protected function createStatement(SqlStatement $statement, \Closure $release): Statement
    {
        \assert($statement instanceof Statement);
        return new PooledStatement($statement, $release);
    }

    protected function createStatementPool(SqlPool $pool, string $sql, \Closure $prepare): StatementPool
    {
        return new StatementPool($pool, $sql, $prepare);
    }

    protected function createTransaction(SqlTransaction $transaction, \Closure $release): Transaction
    {
        return new PooledTransaction($transaction, $release);
    }

    /**
     * Changes return type to this library's Result type.
     */
    public function query(string $sql): Result
    {
        return parent::query($sql);
    }

    /**
     * Changes return type to this library's Statement type.
     */
    public function prepare(string $sql): Statement
    {
        return parent::prepare($sql);
    }

    /**
     * Changes return type to this library's Result type.
     */
    public function execute(string $sql, array $params = []): Result
    {
        return parent::execute($sql, $params);
    }

    /**
     * Changes return type to this library's Transaction type.
     */
    public function beginTransaction(TransactionIsolation $isolation = TransactionIsolation::Committed): Transaction
    {
        return parent::beginTransaction($isolation);
    }
}
