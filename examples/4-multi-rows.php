<?php

require 'support/bootstrap.php';
require 'support/generic-table.php';

use Amp\Mysql;

Amp\Loop::run(function () {
    $db = Mysql\pool(Mysql\ConnectionConfig::fromString("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=".DB_NAME));

    /* create same table than in 3-generic-with-yield.php */
    yield from createGenericTable($db);

    $promises = [];

    $promises[] = $db->query("SELECT a * b FROM tmp");
    $promises[] = $db->execute("SELECT POW(a, ?) AS power FROM tmp", [2]);

    /**
     * @var Mysql\ResultSet $result1
     * @var Mysql\ResultSet $result2
     */
    list($result1, $result2) = yield $promises; // Both queries execute simultaneously. Wait for both to finish here.

    print "Query 1 Results:" . PHP_EOL;
    while (yield $result1->advance()) {
        \var_dump($result1->getCurrent());
    }

    print  PHP_EOL . "Query 2 Results:" . PHP_EOL;
    while (yield $result2->advance()) {
        \var_dump($result2->getCurrent());
    }

    yield $db->query("DROP TABLE tmp");

    $db->close();
});
