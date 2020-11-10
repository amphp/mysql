<?php

namespace Amp\Mysql;

use Amp\Sql\Common\ConnectionPool;
use Amp\Sql\Connector;
use Amp\Sql\Pool as SqlPool;
use Amp\Sql\Result as SqlResult;
use Amp\Sql\Statement as SqlStatement;
use Amp\Sql\Transaction as SqlTransaction;

final class Pool extends ConnectionPool
{
    protected function createDefaultConnector(): Connector
    {
        return connector();
    }

    protected function createResult(SqlResult $result, callable $release): Result
    {
        \assert($result instanceof Result);
        return new PooledResult($result, $release);
    }

    protected function createStatement(SqlStatement $statement, callable $release): Statement
    {
        \assert($statement instanceof Statement);
        return new PooledStatement($statement, $release);
    }

    protected function createStatementPool(SqlPool $pool, string $sql, callable $prepare): StatementPool
    {
        return new StatementPool($pool, $sql, $prepare);
    }

    protected function createTransaction(SqlTransaction $transaction, callable $release): SqlTransaction
    {
        return new PooledTransaction($transaction, $release);
    }
}
