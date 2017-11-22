<?php

require 'support/bootstrap.php';
require 'support/generic-table.php';

Amp\Loop::run(function () {
    $db = Amp\Mysql\pool("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=".DB_NAME);

    /* create same table than in 3-generic-with-yield.php */
    yield from createGenericTable($db);

    /** @var \Amp\Mysql\Transaction $transaction */
    $transaction = yield $db->transaction();

    yield $transaction->execute("INSERT INTO tmp VALUES (?, ? * 2)", [6, 6]);

    /** @var \Amp\Mysql\ResultSet $result */
    $result = yield $transaction->execute("SELECT * FROM tmp WHERE a >= ?", [5]); // Two rows should be returned.

    while (yield $result->advance()) {
        var_dump($result->getCurrent());
    }

    yield $transaction->rollback();

    // Run same query again, should only return a single row since the other was rolled back.
    $result = yield $db->execute("SELECT * FROM tmp WHERE a >= ?", [5]);

    while (yield $result->advance()) {
        var_dump($result->getCurrent());
    }

    $db->close();
});
