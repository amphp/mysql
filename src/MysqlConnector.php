<?php

namespace Amp\Mysql;

use Amp\Cancellation;
use Amp\Sql\SqlConfig;
use Amp\Sql\SqlConnector;
use Amp\Sql\SqlException;

/**
 * @extends SqlConnector<MysqlConfig, MysqlConnection>
 */
interface MysqlConnector extends SqlConnector
{
    /**
     * @throws SqlException
     */
    public function connect(SqlConfig $config, ?Cancellation $cancellation = null): MysqlLink;
}
