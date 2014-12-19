<?php

require '../vendor/autoload.php';

const DB_HOST = "",
      DB_USER = "",
      DB_PASS = "",
      DB_NAME = "";

\Amp\run(function() {
	$db = new \Mysql\Pool("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=".DB_NAME);

	/* Create table and insert a few rows */
	/* we need to wait until table is finished, so that we can insert. */
	yield $db->query("CREATE TABLE tmp SELECT 1 AS a, 2 AS b");

	$promises = [];
	foreach (range(1, 5) as $num) {
		$promises[] = $db->query("INSERT INTO tmp (a, b) VALUES ($num, $num * 2)");
	}

	/* wait until everything is inserted (in case where we wouldn't have to wait, we also could just  */
	yield $promises;

});