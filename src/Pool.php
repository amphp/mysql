<?php

namespace Amp\Mysql;

use Amp\Sql\AbstractPool;
use Amp\Sql\Connector;
use Amp\Sql\Pool as SqlPool;
use Amp\Sql\PooledResultSet;
use Amp\Sql\ResultSet as SqlResultSet;
use Amp\Sql\Statement;
use Amp\Sql\StatementPool as SqlStatementPool;
use Amp\Sql\Transaction as SqlTransaction;

final class Pool extends AbstractPool
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

    protected function createStatement(Statement $statement, callable $release): Statement
    {
        \assert($statement instanceof ConnectionStatement);
        return new PooledStatement($statement, $release);
    }

    protected function createStatementPool(SqlPool $pool, Statement $statement, callable $prepare): SqlStatementPool
    {
        return new StatementPool($pool, $statement, $prepare);
    }

    protected function createTransaction(SqlTransaction $transaction, callable $release): SqlTransaction
    {
        \assert($transaction instanceof Transaction);
        return new PooledTransaction($transaction, $release);
    }
}
