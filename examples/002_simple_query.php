<?php

require '../vendor/autoload.php';

const DB_HOST = "",
      DB_USER = "",
      DB_PASS = "",
      DB_NAME = "";

\Amp\run(function() {
	$db = new \Mysql\Pool("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=".DB_NAME);

	/* yeah, we need a lot of yields and assigns here... With PHP 7 we finally can drop a lot of stupid parenthesis! */
	$query = (yield $db->query("SELECT 1"));
	list($one) = (yield $query->fetchRow());

	var_dump($one); // should output string(1) "1"
});