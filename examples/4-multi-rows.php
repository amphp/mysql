<?php

require 'support/bootstrap.php';
require 'support/generic-table.php';

Amp\Loop::run(function() {
    $db = Amp\Mysql\pool("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=".DB_NAME);

    /* create same table than in 3-generic-with-yield.php */
    yield from createGenericTable($db);

    $promises = [];

    /* yeah, we need a lot of yields and assigns here... */
    $promises[] = $db->query("SELECT a * b FROM tmp");

    /* or, maybe, wait until they're all fetched (because you anyway only can continue after having full resultset */
    $promises[] = $db->query("SELECT a * b FROM tmp");

    list($result1, $result2) = yield $promises;

    while ((list($result) = yield $result1->fetchRow()) !== null) {
        var_dump($result);
    }

    yield $db->query("DROP TABLE tmp");

    $db->close();
});
