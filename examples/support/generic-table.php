<?php

/* Create table and fill in a few rows for examples; for comments see 003_generic_with_yield.php */
function createGenericTable(\Amp\Mysql\Pool $db): Generator {
    yield $db->query("CREATE TABLE tmp SELECT 1 AS a, 2 AS b");
    $promises = [];
    foreach (range(1, 5) as $num) {
        $promises[] = $db->query("INSERT INTO tmp (a, b) VALUES ($num, $num * 2)");
    }
    return yield $promises;
}