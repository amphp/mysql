<?php

use Amp\Future;
use Amp\Sql\Link;

/* Create table and fill in a few rows for examples; for comments see 3-generic-with-yield.php */
function createGenericTable(Link $db): void
{
    $db->query("DROP TABLE IF EXISTS tmp");

    $db->query("CREATE TABLE tmp (a INT(10), b INT(10))");

    $statement = $db->prepare("INSERT INTO tmp (a, b) VALUES (?, ? * 2)");

    $futures = [];
    foreach (\range(1, 5) as $num) {
        $futures[] = \Amp\async(fn() => $statement->execute([$num, $num]));
    }

    Future\all($futures);
}
