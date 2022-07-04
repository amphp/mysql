<?php

namespace Amp\Mysql;

use Amp\Sql\Link;
use Amp\Sql\TransactionIsolation;
use Amp\Sql\TransactionIsolationLevel;

interface MysqlLink extends MysqlExecutor, Link
{
    /**
     * @return MysqlTransaction Transaction object specific to this library.
     */
    public function beginTransaction(
        TransactionIsolation $isolation = TransactionIsolationLevel::Committed
    ): MysqlTransaction;
}
