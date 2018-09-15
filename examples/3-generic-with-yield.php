<?php

require 'support/bootstrap.php';

use Amp\Mysql;

Amp\Loop::run(function () {
    $db = Mysql\pool(Mysql\ConnectionConfig::fromString("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=".DB_NAME));

    /* Create table and insert a few rows */
    /* we need to wait until table is finished, so that we can insert. */
    yield $db->query("CREATE TABLE IF NOT EXISTS tmp (a INT(10), b INT(10))");

    print "Table successfully created." . PHP_EOL;

    /** @var Mysql\Statement $statement */
    $statement = yield $db->prepare("INSERT INTO tmp (a, b) VALUES (?, ? * 2)");

    $promises = [];
    foreach (\range(1, 5) as $num) {
        $promises[] = $statement->execute([$num, $num]);
    }

    /* wait until everything is inserted */
    yield $promises;

    print "Insertion successful (if it wasn't, an exception would have been thrown by now)" . PHP_EOL;

    /** @var Mysql\ResultSet $result */
    $result = yield $db->query("SELECT a, b FROM tmp");

    while (yield $result->advance()) {
        \var_dump($result->getCurrent());
    }

    yield $db->query("DROP TABLE tmp");

    $db->close();
});
