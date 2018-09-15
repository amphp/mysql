<?php

require 'support/bootstrap.php';
require 'support/generic-table.php';

use Amp\Mysql;

\Amp\Loop::run(function () {
    $db = Mysql\pool(Mysql\ConnectionConfig::fromString("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=".DB_NAME));

    /* create same table than in 3-generic-with-yield.php */
    yield from createGenericTable($db);

    /* multi statements are enabled by default, but generally stored procedures also might return multiple resultsets anyway */
    $result = yield $db->query("SELECT a + b FROM tmp; SELECT a - b FROM tmp;");

    $i = 0;
    /** @var Mysql\ResultSet $result */
    do {
        print PHP_EOL . "Query " . ++$i . " Results:" . PHP_EOL;
        while (yield $result->advance()) {
            \var_dump($result->getCurrent());
        }
    } while (yield $result->nextResultSet()); // Advances to the next result set.

    yield $db->query("DROP TABLE tmp");

    $db->close();
});
