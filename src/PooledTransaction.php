<?php

namespace Amp\Mysql;

use Amp\Sql\Common\PooledTransaction as SqlPooledTransaction;
use Amp\Sql\ResultSet as SqlResultSet;
use Amp\Sql\Statement as SqlStatement;

final class PooledTransaction extends SqlPooledTransaction
{
    protected function createStatement(SqlStatement $statement, callable $release): SqlStatement
    {
        \assert($statement instanceof Statement);
        return new PooledStatement($statement, $release);
    }

    protected function createResultSet(SqlResultSet $resultSet, callable $release): SqlResultSet
    {
        \assert($resultSet instanceof ResultSet);
        return new PooledResultSet($resultSet, $release);
    }
}
