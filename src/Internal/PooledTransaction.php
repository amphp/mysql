<?php

namespace Amp\Mysql\Internal;

use Amp\Mysql\Result;
use Amp\Mysql\Statement;
use Amp\Mysql\Transaction;
use Amp\Sql\Common\PooledTransaction as SqlPooledTransaction;
use Amp\Sql\Result as SqlResult;
use Amp\Sql\Statement as SqlStatement;

/** @internal */
final class PooledTransaction extends SqlPooledTransaction implements Transaction
{
    protected function createStatement(SqlStatement $statement, \Closure $release): Statement
    {
        \assert($statement instanceof Statement);
        return new PooledStatement($statement, $release);
    }

    protected function createResult(SqlResult $result, \Closure $release): Result
    {
        \assert($result instanceof Result);
        return new PooledResult($result, $release);
    }

    /**
     * Changes return type to this library's Result type.
     *
     * @psalm-suppress LessSpecificReturnStatement, MoreSpecificReturnType
     */
    public function query(string $sql): Result
    {
        return parent::query($sql);
    }

    /**
     * Changes return type to this library's Statement type.
     *
     * @psalm-suppress LessSpecificReturnStatement, MoreSpecificReturnType
     */
    public function prepare(string $sql): Statement
    {
        return parent::prepare($sql);
    }

    /**
     * Changes return type to this library's Result type.
     *
     * @psalm-suppress LessSpecificReturnStatement, MoreSpecificReturnType
     */
    public function execute(string $sql, array $params = []): Result
    {
        return parent::execute($sql, $params);
    }
}
