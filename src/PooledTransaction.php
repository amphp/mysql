<?php

namespace Amp\Mysql;

use Amp\Sql\Common\PooledTransaction as SqlPooledTransaction;
use Amp\Sql\Result as SqlResult;
use Amp\Sql\Statement as SqlStatement;

final class PooledTransaction extends SqlPooledTransaction
{
    protected function createStatement(SqlStatement $statement, callable $release): SqlStatement
    {
        if (!$statement instanceof Statement) {
            throw new \TypeError('Statement object must be an instance of ' . Statement::class);
        }
        return new PooledStatement($statement, $release);
    }

    protected function createResult(SqlResult $result, callable $release): SqlResult
    {
        if (!$result instanceof Result) {
            throw new \TypeError('Result object must be an instance of ' . Result::class);
        }
        return new PooledResult($result, $release);
    }
}
