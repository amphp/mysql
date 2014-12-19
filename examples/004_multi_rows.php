<?php

require '../vendor/autoload.php';
require 'support/generic_table.php';

const DB_HOST = "",
      DB_USER = "",
      DB_PASS = "",
      DB_NAME = "";

\Amp\run(function() {
	$db = new \Mysql\Pool("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=".DB_NAME);

	/* create same table than in 003_generic_with_yield.php */
	yield genTable($db);

	/* yeah, we need a lot of yields and assigns here... With PHP 7 we finally can drop a lot of stupid parenthesis! */
	$query = (yield $db->query("SELECT a * b FROM tmp"));
	while ((list($result) = (yield $query->fetchRow())) !== null) {
		var_dump($result); // outputs each row of the resultset returned by SELECT a * b FROM tmp
	}

	/* or, maybe, wait until they're all fetched (because you anyway only can continue after having full resultset */
	$query = (yield $db->query("SELECT a * b FROM tmp"));
	$objs = (yield $query->fetchObjects());
	var_dump($objs); // outputs all the rows as objects of the resultset returned by SELECT a * b FROM tmp
});