<?php

namespace Amp\Mysql;

use Amp\Sql\Transaction;

interface MysqlTransaction extends MysqlExecutor, Transaction
{
}
