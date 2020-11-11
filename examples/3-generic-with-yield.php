<?php

require 'support/bootstrap.php';

use Amp\Mysql;
use function Amp\async;
use function Amp\await;

$db = Mysql\pool(Mysql\ConnectionConfig::fromString("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=".DB_NAME));

/* Create table and insert a few rows */
/* we need to wait until table is finished, so that we can insert. */
$db->query("CREATE TABLE IF NOT EXISTS tmp (a INT(10), b INT(10))");

print "Table successfully created." . PHP_EOL;

$statement = $db->prepare("INSERT INTO tmp (a, b) VALUES (?, ? * 2)");

$promises = [];
foreach (\range(1, 5) as $num) {
    $promises[] = async(fn() => $statement->execute([$num, $num]));
}

/* wait until everything is inserted */
await($promises);

print "Insertion successful (if it wasn't, an exception would have been thrown by now)" . PHP_EOL;

$result = $db->query("SELECT a, b FROM tmp");

foreach ($result as $row) {
    \var_dump($row);
}

$db->query("DROP TABLE tmp");

$db->close();
