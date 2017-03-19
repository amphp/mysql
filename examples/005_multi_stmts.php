<?php

require './example_bootstrap.php';
require 'support/generic_table.php';

\Amp\Loop::run(function() {
	$db = new \Amp\Mysql\Pool("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=".DB_NAME);

	/* create same table than in 003_generic_with_yield.php */
	yield new \Amp\Coroutine(genTable($db));

	/* multi statements are enabled by default, but generally stored procedures also might return multiple resultsets anyway */
	$promise = $db->query("SELECT a + b FROM tmp; SELECT a - b FROM tmp;");

	for ($rows = (yield $promise); $rows !== null; $rows = (yield $rows->next())) {
		while (($row = (yield $rows->fetch())) !== null) {
			var_dump($row); // associative array paired with numeric indices
		}

		/* be aware that it is *not* optimal to fetch rowCount before fetching data. We only know rowCount when all data has been fetched! So, it has to fetch everything first before knowing rowCount. */
		var_dump(yield $rows->rowCount());
	}

	$db->close();
});