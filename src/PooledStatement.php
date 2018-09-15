<?php

namespace Amp\Mysql;

use Amp\Sql\PooledStatement as SqlPooledStatement;
use Amp\Sql\ResultSet as SqlResultSet;

final class PooledStatement extends SqlPooledStatement
{
    protected function createResultSet(SqlResultSet $resultSet, callable $release): SqlResultSet
    {
        \assert($resultSet instanceof ResultSet);
        return new PooledResultSet($resultSet, $release);
    }
}
