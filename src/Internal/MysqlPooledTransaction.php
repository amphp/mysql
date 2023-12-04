<?php declare(strict_types=1);

namespace Amp\Mysql\Internal;

use Amp\Mysql\MysqlResult;
use Amp\Mysql\MysqlStatement;
use Amp\Mysql\MysqlTransaction;
use Amp\Sql\Common\PooledTransaction;
use Amp\Sql\Transaction;

/**
 * @internal
 * @extends PooledTransaction<MysqlResult, MysqlStatement, MysqlTransaction>
 */
final class MysqlPooledTransaction extends PooledTransaction implements MysqlTransaction
{
    use MysqlTransactionDelegate;

    /**
     * @param \Closure():void $release
     */
    public function __construct(private readonly MysqlTransaction $transaction, \Closure $release)
    {
        parent::__construct($transaction, $release);
    }

    protected function createTransaction(Transaction $transaction, \Closure $release): Transaction
    {
        \assert($transaction instanceof MysqlTransaction);
        return new MysqlPooledTransaction($transaction, $release);
    }
}
