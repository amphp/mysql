<?php

require 'support/bootstrap.php';

use Amp\Mysql\ResultSet;

Amp\Loop::run(function () {
    $db = Amp\Mysql\pool("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=".DB_NAME);

    /** @var \Amp\Mysql\ResultSet $result */
    $result = yield $db->query("SELECT 1 AS value");

    while (yield $result->advance(ResultSet::FETCH_ARRAY)) {
        $row = $result->getCurrent();
        var_dump($row[0]);
    }

    $db->close();
});
