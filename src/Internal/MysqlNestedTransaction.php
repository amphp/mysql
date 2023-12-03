<?php declare(strict_types=1);

namespace Amp\Mysql\Internal;

use Amp\Mysql\MysqlResult;
use Amp\Mysql\MysqlStatement;
use Amp\Mysql\MysqlTransaction;
use Amp\Sql\Common\NestableTransactionExecutor;
use Amp\Sql\Common\NestedTransaction;
use Amp\Sql\Transaction;

/**
 * @internal
 * @extends NestedTransaction<MysqlResult, MysqlStatement, MysqlTransaction, MysqlNestableExecutor>
 */
final class MysqlNestedTransaction extends NestedTransaction implements MysqlTransaction
{
    use MysqlTransactionDelegate;

    /**
     * @param non-empty-string $identifier
     * @param \Closure():void $release
     */
    public function __construct(
        private readonly MysqlTransaction $transaction,
        MysqlNestableExecutor $executor,
        string $identifier,
        \Closure $release,
    ) {
        parent::__construct($transaction, $executor, $identifier, $release);
    }

    protected function getTransaction(): MysqlTransaction
    {
        return $this->transaction;
    }

    protected function createNestedTransaction(
        Transaction $transaction,
        NestableTransactionExecutor $executor,
        string $identifier,
        \Closure $release,
    ): MysqlTransaction {
        return new self($transaction, $executor, $identifier, $release);
    }
}
