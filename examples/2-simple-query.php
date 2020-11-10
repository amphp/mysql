<?php

require 'support/bootstrap.php';

use Amp\Mysql;

$db = Mysql\pool(Mysql\ConnectionConfig::fromString("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=".DB_NAME));

$result = $db->query("SELECT 1 AS value");

while ($row = $result->continue()) {
    \var_dump($row['value']);
}

$db->close();
