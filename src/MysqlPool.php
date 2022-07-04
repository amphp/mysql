<?php

namespace Amp\Mysql;

use Amp\Sql\Common\ConnectionPool;
use Amp\Sql\Common\StatementPool;
use Amp\Sql\Pool as SqlPool;
use Amp\Sql\Result as SqlResult;
use Amp\Sql\Statement as SqlStatement;
use Amp\Sql\Transaction as SqlTransaction;
use Amp\Sql\TransactionIsolation;
use Amp\Sql\TransactionIsolationLevel;

final class MysqlPool extends ConnectionPool implements MysqlLink
{
    /**
     * @param positive-int $maxConnections
     * @param positive-int $idleTimeout
     */
    public function __construct(
        MysqlConfig $config,
        int $maxConnections = self::DEFAULT_MAX_CONNECTIONS,
        int $idleTimeout = self::DEFAULT_IDLE_TIMEOUT,
        ?MysqlConnector $connector = null,
    ) {
        parent::__construct($config, $connector ?? mysqlConnector(), $maxConnections, $idleTimeout);
    }

    protected function createResult(SqlResult $result, \Closure $release): MysqlResult
    {
        \assert($result instanceof MysqlResult);
        return new Internal\MysqlPooledResult($result, $release);
    }

    protected function createStatement(SqlStatement $statement, \Closure $release): MysqlStatement
    {
        \assert($statement instanceof MysqlStatement);
        return new Internal\MysqlPooledStatement($statement, $release);
    }

    protected function createStatementPool(SqlPool $pool, string $sql, \Closure $prepare): StatementPool
    {
        return new Internal\MysqlStatementPool($pool, $sql, $prepare);
    }

    protected function createTransaction(SqlTransaction $transaction, \Closure $release): MysqlTransaction
    {
        return new Internal\MysqlPooledTransaction($transaction, $release);
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

    /**
     * Changes return type to this library's Transaction type.
     *
     * @psalm-suppress LessSpecificReturnStatement, MoreSpecificReturnType
     */
    public function beginTransaction(
        TransactionIsolation $isolation = TransactionIsolationLevel::Committed
    ): MysqlTransaction {
        return parent::beginTransaction($isolation);
    }
}
