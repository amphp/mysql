<?php

require './example_bootstrap.php';

\Amp\Loop::run(function() {
    $db = new \Amp\Mysql\Pool("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=".DB_NAME);

    $query = yield $db->query("SELECT 1");
    list($one) = yield $query->fetchRow();

    var_dump($one); // should output string(1) "1"

    $db->close();
});