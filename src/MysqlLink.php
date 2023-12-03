<?php declare(strict_types=1);

namespace Amp\Mysql;

use Amp\Sql\Link;

/**
 * @extends Link<MysqlResult, MysqlStatement, MysqlTransaction>
 */
interface MysqlLink extends MysqlExecutor, Link
{
    /**
     * @return MysqlTransaction Transaction object specific to this library.
     */
    public function beginTransaction(): MysqlTransaction;
}
