<?php

require 'support/bootstrap.php';

use Amp\Mysql\MysqlConfig;
use Amp\Mysql\MysqlPool;

$db = new MysqlPool(MysqlConfig::fromString("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=".DB_NAME));

$result = $db->query("SELECT 1 AS value");

foreach ($result as $row) {
    \var_dump($row['value']);
}

$db->close();
