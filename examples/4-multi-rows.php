<?php

require 'support/bootstrap.php';
require 'support/generic-table.php';

\Amp\Loop::run(function() {
    $db = new \Amp\Mysql\Pool("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=".DB_NAME);

    /* create same table than in 3-generic-with-yield.php */
    yield from createGenericTable($db);

    /* yeah, we need a lot of yields and assigns here... With PHP 7 we finally can drop a lot of stupid parenthesis! */
    $query = (yield $db->query("SELECT a * b FROM tmp"));
    while ((list($result) = (yield $query->fetchRow())) !== null) {
        var_dump($result); // outputs each row of the resultset returned by SELECT a * b FROM tmp
    }

    /* or, maybe, wait until they're all fetched (because you anyway only can continue after having full resultset */
    $query = (yield $db->query("SELECT a * b FROM tmp"));
    $objs = (yield $query->fetchObjects());
    var_dump($objs); // outputs all the rows as objects of the resultset returned by SELECT a * b FROM tmp

    yield $db->query("DROP TABLE tmp");

    $db->close();
});