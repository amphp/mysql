<?php

require 'support/bootstrap.php';
require 'support/generic-table.php';

\Amp\Loop::run(function() {
    $db = \Amp\Mysql\pool("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=".DB_NAME);

    /* create same table than in 3-generic-with-yield.php */
    yield from createGenericTable($db);

    /* multi statements are enabled by default, but generally stored procedures also might return multiple resultsets anyway */
    $promise = $db->query("SELECT a + b FROM tmp; SELECT a - b FROM tmp;");

    for ($rows = yield $promise; $rows !== null; $rows = yield $rows->next()) {
        while (($row = yield $rows->fetch()) !== null) {
            var_dump($row); // associative array paired with numeric indices
        }

        /* be aware that it is *not* optimal to fetch rowCount before fetching data. We only know rowCount when all data has been fetched! So, it has to fetch everything first before knowing rowCount. */
        var_dump(yield $rows->rowCount());
    }

    $db->query("DROP TABLE tmp");

    $db->close();
});
