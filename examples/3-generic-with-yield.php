<?php

require 'support/bootstrap.php';

Amp\Loop::run(function() {
    $db = new Amp\Mysql\Pool("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=".DB_NAME);

    /* Create table and insert a few rows */
    /* we need to wait until table is finished, so that we can insert. */
    yield $db->query("CREATE TABLE IF NOT EXISTS tmp SELECT 1 AS a, 2 AS b");

    print "Table successfully created." . PHP_EOL;

    $promises = [];
    foreach (range(1, 5) as $num) {
        $promises[] = $db->prepare("INSERT INTO tmp (a, b) VALUES (?, ? * 2)", [$num, $num]);
    }

    /* wait until everything is inserted */
    yield $promises;

    print "Insertion successful (if it wasn't, an exception would have been thrown by now)" . PHP_EOL;

    $db->close();
});
