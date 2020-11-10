<?php

require 'support/bootstrap.php';
require 'support/generic-table.php';

use Amp\Mysql;

$db = Mysql\pool(Mysql\ConnectionConfig::fromString("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=".DB_NAME));

/* create same table than in 3-generic-with-yield.php */
createGenericTable($db);

$transaction = $db->beginTransaction();

$transaction->execute("INSERT INTO tmp VALUES (?, ? * 2)", [6, 6]);

$result = $transaction->execute("SELECT * FROM tmp WHERE a >= ?", [5]); // Two rows should be returned.

while ($row = $result->continue()) {
    \var_dump($row);
}

$transaction->rollback();

// Run same query again, should only return a single row since the other was rolled back.
$result = $db->execute("SELECT * FROM tmp WHERE a >= ?", [5]);

while ($row = $result->continue()) {
    \var_dump($row);
}

$db->close();
