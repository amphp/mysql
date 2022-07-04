<?php

namespace Amp\Mysql;

use Amp\Cancellation;
use Amp\Sql\SqlConfig;
use Amp\Sql\SqlConnector;
use Amp\Sql\SqlException;

interface MysqlConnector extends SqlConnector
{
    /**
     * @throws SqlException
     */
    public function connect(SqlConfig $config, ?Cancellation $cancellation = null): MysqlConnection;
}
