<?php

/* Create table and fill in a few rows for examples; for comments see 3-generic-with-yield.php */
function createGenericTable(\Amp\Sql\Link $db): Generator
{
    yield $db->query("DROP TABLE IF EXISTS tmp");

    yield $db->query("CREATE TABLE tmp (a INT(10), b INT(10))");

    $statement = yield $db->prepare("INSERT INTO tmp (a, b) VALUES (?, ? * 2)");

    $promises = [];
    foreach (\range(1, 5) as $num) {
        $promises[] = $statement->execute([$num, $num]);
    }

    return yield $promises;
}
