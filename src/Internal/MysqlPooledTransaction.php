<?php

namespace Amp\Mysql\Internal;

use Amp\Mysql\MysqlResult;
use Amp\Mysql\MysqlStatement;
use Amp\Mysql\MysqlTransaction;
use Amp\Sql\Common\PooledTransaction as SqlPooledTransaction;
use Amp\Sql\Result as SqlResult;
use Amp\Sql\Statement as SqlStatement;

/** @internal */
final class MysqlPooledTransaction extends SqlPooledTransaction implements MysqlTransaction
{
    protected function createStatement(SqlStatement $statement, \Closure $release): MysqlStatement
    {
        \assert($statement instanceof MysqlStatement);
        return new MysqlPooledStatement($statement, $release);
    }

    protected function createResult(SqlResult $result, \Closure $release): MysqlResult
    {
        \assert($result instanceof MysqlResult);
        return new MysqlPooledResult($result, $release);
    }

    /**
     * Changes return type to this library's Result type.
     *
     * @psalm-suppress LessSpecificReturnStatement, MoreSpecificReturnType
     */
    public function query(string $sql): MysqlResult
    {
        return parent::query($sql);
    }

    /**
     * Changes return type to this library's Statement type.
     *
     * @psalm-suppress LessSpecificReturnStatement, MoreSpecificReturnType
     */
    public function prepare(string $sql): MysqlStatement
    {
        return parent::prepare($sql);
    }

    /**
     * Changes return type to this library's Result type.
     *
     * @psalm-suppress LessSpecificReturnStatement, MoreSpecificReturnType
     */
    public function execute(string $sql, array $params = []): MysqlResult
    {
        return parent::execute($sql, $params);
    }
}
