<?php declare(strict_types=1);

namespace Amp\Mysql\Internal;

use Amp\Mysql\MysqlResult;
use Amp\Mysql\MysqlStatement;
use Amp\Mysql\MysqlTransaction;
use Amp\Sql\Common\ConnectionTransaction;
use Amp\Sql\Common\NestableTransactionExecutor;
use Amp\Sql\Transaction;

/**
 * @internal
 * @extends ConnectionTransaction<MysqlResult, MysqlStatement, MysqlTransaction, MysqlNestableExecutor>
 */
final class MysqlConnectionTransaction extends ConnectionTransaction implements MysqlTransaction
{
    use MysqlTransactionDelegate;

    protected function createNestedTransaction(
        Transaction $transaction,
        NestableTransactionExecutor $executor,
        string $identifier,
        \Closure $release,
    ): MysqlTransaction {
        \assert($transaction instanceof MysqlTransaction);
        return new MysqlNestedTransaction($transaction, $executor, $identifier, $release);
    }
}
