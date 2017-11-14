<?php

require 'support/bootstrap.php';
require 'support/generic-table.php';

\Amp\Loop::run(function() {
    $db = \Amp\Mysql\pool("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=".DB_NAME);

    /* create same table than in 3-generic-with-yield.php */
    yield from createGenericTable($db);

    /* multi statements are enabled by default, but generally stored procedures also might return multiple resultsets anyway */
    $result = yield $db->query("SELECT a + b FROM tmp; SELECT a - b FROM tmp;");

    /** @var \Amp\Mysql\ResultSet $result */
    while (yield $result->advance()) {
        var_dump($result->getCurrent());
    }

    yield $db->query("DROP TABLE tmp");

    $db->close();
});
