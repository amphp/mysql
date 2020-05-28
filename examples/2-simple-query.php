<?php

require 'support/bootstrap.php';

use Amp\Mysql;

Amp\Loop::run(function () {
    $db = Mysql\pool(Mysql\ConnectionConfig::fromString("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=".DB_NAME));

    /** @var Mysql\Result $result */
    $result = yield $db->query("SELECT 1 AS value");

    while ($row = yield $result->continue()) {
        \var_dump($row['value']);
    }

    $db->close();
});
