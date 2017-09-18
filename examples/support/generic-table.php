<?php

/* Create table and fill in a few rows for examples; for comments see 3-generic-with-yield.php */
function createGenericTable(\Amp\Mysql\Pool $db): Generator {
    yield $db->query("CREATE TABLE IF NOT EXISTS tmp SELECT 1 AS a, 2 AS b");
    $promises = [];
    foreach (range(1, 5) as $num) {
        $promises[] = $db->prepare("INSERT INTO tmp (a, b) VALUES (?, ? * 2)", [$num, $num]);
    }
    return yield $promises;
}
