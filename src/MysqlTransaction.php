<?php declare(strict_types=1);

namespace Amp\Mysql;

use Amp\Sql\Transaction;

/**
 * @extends Transaction<MysqlResult, MysqlStatement, MysqlTransaction>
 */
interface MysqlTransaction extends MysqlLink, Transaction
{
}
