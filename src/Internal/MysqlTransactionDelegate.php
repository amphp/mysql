<?php declare(strict_types=1);

namespace Amp\Mysql\Internal;

use Amp\Mysql\MysqlResult;
use Amp\Mysql\MysqlStatement;
use Amp\Mysql\MysqlTransaction;
use Amp\Sql\Result;
use Amp\Sql\Statement;

/** @internal */
trait MysqlTransactionDelegate
{
    abstract protected function getTransaction(): MysqlTransaction;

    protected function createStatement(Statement $statement, \Closure $release): MysqlStatement
    {
        \assert($statement instanceof MysqlStatement);
        return new MysqlPooledStatement($statement, $release);
    }

    protected function createResult(Result $result, \Closure $release): MysqlResult
    {
        \assert($result instanceof MysqlResult);
        return new MysqlPooledResult($result, $release);
    }

    /**
     * Changes return type to this library's Result type.
     */
    public function query(string $sql): MysqlResult
    {
        return parent::query($sql);
    }

    /**
     * Changes return type to this library's Statement type.
     */
    public function prepare(string $sql): MysqlStatement
    {
        return parent::prepare($sql);
    }

    /**
     * Changes return type to this library's Result type.
     */
    public function execute(string $sql, array $params = []): MysqlResult
    {
        return parent::execute($sql, $params);
    }
}