<?php

namespace Amp\Mysql;

use Amp\Sql\ResultSet as SqlResultSet;
use Amp\Sql\StatementPool as SqlStatementPool;

final class StatementPool extends SqlStatementPool
{
    protected function createResultSet(SqlResultSet $resultSet, callable $release): SqlResultSet
    {
        \assert(
            $resultSet instanceof ResultSet
            || $resultSet instanceof PooledResultSet
        );
        return new PooledResultSet($resultSet, $release);
    }
}
