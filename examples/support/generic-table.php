<?php

use Amp\Sql\Link;
use function Amp\async;
use function Amp\await;

/* Create table and fill in a few rows for examples; for comments see 3-generic-with-yield.php */
function createGenericTable(Link $db): void
{
    $db->query("DROP TABLE IF EXISTS tmp");

    $db->query("CREATE TABLE tmp (a INT(10), b INT(10))");

    $statement = $db->prepare("INSERT INTO tmp (a, b) VALUES (?, ? * 2)");

    $promises = [];
    foreach (\range(1, 5) as $num) {
        $promises[] = async(fn() => $statement->execute([$num, $num]));
    }

    await($promises);
}
