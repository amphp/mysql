<?php

require 'support/bootstrap.php';
require 'support/generic-table.php';

use Amp\Future;
use Amp\Mysql;
use function Amp\async;

$db = Mysql\pool(Mysql\MysqlConfig::fromString("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=".DB_NAME));

/* create same table than in 3-generic-with-yield.php */
createGenericTable($db);

$future = [];

$future[] = async(fn () => $db->query("SELECT a * b FROM tmp"));
$future[] = async(fn () => $db->execute("SELECT POW(a, ?) AS power FROM tmp", [2]));

try {
    /**
     * @var Mysql\MysqlResult $result1
     * @var Mysql\MysqlResult $result2
     */
    [$result1, $result2] = Future\await($future); // Both queries execute simultaneously. Wait for both to finish here.
} catch (\Throwable $e) {
    var_dump($e);
}

print "Query 1 Results:" . PHP_EOL;
foreach ($result1 as $row) {
    \var_dump($row);
}

print  PHP_EOL . "Query 2 Results:" . PHP_EOL;
foreach ($result2 as $row) {
    \var_dump($row);
}

$db->query("DROP TABLE tmp");

$db->close();
