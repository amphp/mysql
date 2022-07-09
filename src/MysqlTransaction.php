<?php

namespace Amp\Mysql;

use Amp\Sql\Transaction;

/**
 * @extends Transaction<MysqlResult, MysqlStatement>
 */
interface MysqlTransaction extends MysqlExecutor, Transaction
{
}
