<?php declare(strict_types=1);

namespace Amp\Mysql;

use Amp\Sql\Connection;

/**
 * @extends Connection<MysqlConfig, MysqlResult, MysqlStatement, MysqlTransaction>
 */
interface MysqlConnection extends MysqlLink, Connection
{
    /**
     * @return MysqlConfig Config object specific to this library.
     */
    public function getConfig(): MysqlConfig;
}
