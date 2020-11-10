<?php

namespace Amp\Mysql;

use Amp\Sql\Common\PooledTransaction as SqlPooledTransaction;
use Amp\Sql\Result as SqlResult;
use Amp\Sql\Statement as SqlStatement;

final class PooledTransaction extends SqlPooledTransaction
{
    protected function createStatement(SqlStatement $statement, callable $release): Statement
    {
        \assert($statement instanceof Statement);
        return new PooledStatement($statement, $release);
    }

    protected function createResult(SqlResult $result, callable $release): Result
    {
        \assert($result instanceof Result);
        return new PooledResult($result, $release);
    }
}
