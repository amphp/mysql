<?php

require 'support/bootstrap.php';
require 'support/generic-table.php';

use Amp\Mysql;

$db = Mysql\pool(Mysql\ConnectionConfig::fromString("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=".DB_NAME));

/* create same table than in 3-generic-with-yield.php */
createGenericTable($db);

/* multi statements are enabled by default, but generally stored procedures also might return multiple resultsets anyway */
$result = $db->query("SELECT a + b FROM tmp; SELECT a - b FROM tmp;");

$i = 0;
do {
    print PHP_EOL . "Query " . ++$i . " Results:" . PHP_EOL;
    while ($row = $result->continue()) {
        \var_dump($row);
    }
} while ($result = $result->getNextResult()); // Advances to the next result set.

$db->query("DROP TABLE tmp");

$db->close();
