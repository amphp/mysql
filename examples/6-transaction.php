<?php

require 'support/bootstrap.php';
require 'support/generic-table.php';

use Amp\Mysql;

Amp\Loop::run(function () {
    $db = Mysql\pool(Mysql\ConnectionConfig::fromString("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=".DB_NAME));

    /* create same table than in 3-generic-with-yield.php */
    yield from createGenericTable($db);

    /** @var \Amp\Sql\Transaction $transaction */
    $transaction = yield $db->beginTransaction();

    yield $transaction->execute("INSERT INTO tmp VALUES (?, ? * 2)", [6, 6]);

    /** @var Mysql\ResultSet $result */
    $result = yield $transaction->execute("SELECT * FROM tmp WHERE a >= ?", [5]); // Two rows should be returned.

    while (yield $result->advance()) {
        \var_dump($result->getCurrent());
    }

    yield $transaction->rollback();

    // Run same query again, should only return a single row since the other was rolled back.
    $result = yield $db->execute("SELECT * FROM tmp WHERE a >= ?", [5]);

    while (yield $result->advance()) {
        \var_dump($result->getCurrent());
    }

    $db->close();
});
