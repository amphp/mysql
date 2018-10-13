<?php

namespace Amp\Mysql;

use Amp\Sql\Common\ConnectionPool;
use Amp\Sql\Common\StatementPool as SqlStatementPool;
use Amp\Sql\Connector;
use Amp\Sql\Pool as SqlPool;
use Amp\Sql\ResultSet as SqlResultSet;
use Amp\Sql\Statement as SqlStatement;
use Amp\Sql\Transaction as SqlTransaction;

final class Pool extends ConnectionPool
{
    protected function createDefaultConnector(): Connector
    {
        return connector();
    }

    protected function createResultSet(SqlResultSet $resultSet, callable $release): SqlResultSet
    {
        \assert($resultSet instanceof ResultSet);
        return new PooledResultSet($resultSet, $release);
    }

    protected function createStatement(SqlStatement $statement, callable $release): SqlStatement
    {
        \assert($statement instanceof Statement);
        return new PooledStatement($statement, $release);
    }

    protected function createStatementPool(SqlPool $pool, SqlStatement $statement, callable $prepare): SqlStatementPool
    {
        \assert($statement instanceof Statement);
        return new StatementPool($pool, $statement, $prepare);
    }

    protected function createTransaction(SqlTransaction $transaction, callable $release): SqlTransaction
    {
        return new PooledTransaction($transaction, $release);
    }
}
