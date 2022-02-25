<?php

namespace Amp\Mysql;

use Amp\Sql\ConnectionConfig;
use Amp\Sql\Connector as SqlConnector;

interface Connector extends SqlConnector
{
    public function connect(ConnectionConfig $config): Connection;
}
