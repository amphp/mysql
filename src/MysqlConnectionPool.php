<?php declare(strict_types=1);

namespace Amp\Mysql;

use Amp\Sql\Common\SqlCommonConnectionPool;
use Amp\Sql\SqlConnector;
use Amp\Sql\SqlResult;
use Amp\Sql\SqlTransaction;
use Amp\Sql\SqlTransactionIsolation;
use Amp\Sql\SqlTransactionIsolationLevel;

/**
 * @extends SqlCommonConnectionPool<MysqlConfig, MysqlResult, MysqlStatement, MysqlTransaction, MysqlConnection>
 */
final class MysqlConnectionPool extends SqlCommonConnectionPool implements MysqlConnection
{
    use Internal\MysqlStatementCreationDelegate;

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
        SqlTransactionIsolation $transactionIsolation = SqlTransactionIsolationLevel::Committed,
    ) {
        parent::__construct(
            config: $config,
            connector: $connector ?? mysqlConnector(),
            maxConnections: $maxConnections,
            idleTimeout: $idleTimeout,
            transactionIsolation: $transactionIsolation,
        );
    }

    #[\Override]
    protected function createResult(SqlResult $result, \Closure $release): MysqlResult
    {
        \assert($result instanceof MysqlResult);
        return new Internal\MysqlPooledResult($result, $release);
    }

    #[\Override]
    protected function createStatementPool(string $sql, \Closure $prepare): MysqlStatement
    {
        return new Internal\MysqlStatementPool($this, $sql, $prepare);
    }

    #[\Override]
    protected function createTransaction(SqlTransaction $transaction, \Closure $release): MysqlTransaction
    {
        return new Internal\MysqlPooledTransaction($transaction, $release);
    }

    /**
     * Changes return type to this library's configuration type.
     */
    #[\Override]
    public function getConfig(): MysqlConfig
    {
        return parent::getConfig();
    }

    /**
     * Changes return type to this library's Result type.
     */
    #[\Override]
    public function query(string $sql): MysqlResult
    {
        return parent::query($sql);
    }

    /**
     * Changes return type to this library's Statement type.
     */
    #[\Override]
    public function prepare(string $sql): MysqlStatement
    {
        return parent::prepare($sql);
    }

    /**
     * Changes return type to this library's Result type.
     */
    #[\Override]
    public function execute(string $sql, array $params = []): MysqlResult
    {
        return parent::execute($sql, $params);
    }

    /**
     * Changes return type to this library's Transaction type.
     */
    #[\Override]
    public function beginTransaction(): MysqlTransaction
    {
        return parent::beginTransaction();
    }
}
