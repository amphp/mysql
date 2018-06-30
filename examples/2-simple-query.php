<?php

require 'support/bootstrap.php';

use Amp\Mysql;
use Amp\Sql\ResultSet;

Amp\Loop::run(function () {
    $db = Mysql\pool(Mysql\ConnectionConfig::parseConnectionString("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=".DB_NAME));

    /** @var Mysql\ResultSet $result */
    $result = yield $db->query("SELECT 1 AS value");

    while (yield $result->advance(ResultSet::FETCH_ARRAY)) {
        $row = $result->getCurrent();
        \var_dump($row[0]);
    }

    $db->close();
});
