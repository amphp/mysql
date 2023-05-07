<?php declare(strict_types=1);

namespace Amp\Mysql;

use Amp\Sql\Common\NestableTransaction;
use Amp\Sql\Transaction;
use Amp\Sql\TransactionIsolation;
use Amp\Sql\TransactionIsolationLevel;

/**
 * @extends NestableTransaction<MysqlResult, MysqlStatement, MysqlTransaction>
 */
class MysqlNestableTransaction extends NestableTransaction implements MysqlLink
{
    protected function createNestedTransaction(
        Transaction $transaction,
        \Closure $release,
        string $identifier,
    ): Transaction {
        return new Internal\MysqlNestedTransaction($transaction, $release, $identifier);
    }

    /**
     * Changes return type to this library's Transaction type.
     */
    public function beginTransaction(
        TransactionIsolation $isolation = TransactionIsolationLevel::Committed
    ): MysqlTransaction {
        return parent::beginTransaction($isolation);
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
