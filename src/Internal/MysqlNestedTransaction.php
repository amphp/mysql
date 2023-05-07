<?php declare(strict_types=1);

namespace Amp\Mysql\Internal;

use Amp\Mysql\MysqlResult;
use Amp\Mysql\MysqlStatement;
use Amp\Mysql\MysqlTransaction;
use Amp\Sql\Common\NestedTransaction;

/**
 * @internal
 * @extends NestedTransaction<MysqlResult, MysqlStatement, MysqlTransaction>
 */
class MysqlNestedTransaction extends NestedTransaction implements MysqlTransaction
{
    use MysqlTransactionDelegate;

    protected function getTransaction(): MysqlTransaction
    {
        return $this->transaction;
    }
}
