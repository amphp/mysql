<?php

namespace Amp\Mysql;

use Amp\Sql\Common\ConnectionPool;
use Amp\Sql\Common\StatementPool;
use Amp\Sql\Pool;
use Amp\Sql\Result;
use Amp\Sql\SqlConnector;
use Amp\Sql\Statement;
use Amp\Sql\Transaction;
use Amp\Sql\TransactionIsolation;
use Amp\Sql\TransactionIsolationLevel;

/**
 * @extends ConnectionPool<MysqlConfig, MysqlConnection, MysqlResult, MysqlStatement, MysqlTransaction>
 */
final class MysqlPool extends ConnectionPool implements MysqlLink
{
    /**
     * @param positive-int $maxConnections
     * @param positive-int $idleTimeout
     * @param SqlConnector<MysqlConfig, MysqlConnection>|null $connector
     */
    public function __construct(
        MysqlConfig $config,
        int $maxConnections = self::DEFAULT_MAX_CONNECTIONS,
        int $idleTimeout = self::DEFAULT_IDLE_TIMEOUT,
        ?SqlConnector $connector = null,
    ) {
        parent::__construct($config, $connector ?? mysqlConnector(), $maxConnections, $idleTimeout);
    }

    protected function createResult(Result $result, \Closure $release): MysqlResult
    {
        \assert($result instanceof MysqlResult);
        return new Internal\MysqlPooledResult($result, $release);
    }

    protected function createStatement(Statement $statement, \Closure $release): MysqlStatement
    {
        \assert($statement instanceof MysqlStatement);
        return new Internal\MysqlPooledStatement($statement, $release);
    }

    protected function createStatementPool(Pool $pool, string $sql, \Closure $prepare): StatementPool
    {
        return new Internal\MysqlStatementPool($pool, $sql, $prepare);
    }

    protected function createTransaction(Transaction $transaction, \Closure $release): MysqlTransaction
    {
        return new Internal\MysqlPooledTransaction($transaction, $release);
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

    /**
     * Changes return type to this library's Transaction type.
     */
    public function beginTransaction(
        TransactionIsolation $isolation = TransactionIsolationLevel::Committed
    ): MysqlTransaction {
        return parent::beginTransaction($isolation);
    }
}
