<?php

/*
 * Generic example for establishing a connection
 */

require '../vendor/autoload.php';

const DB_HOST = "",
      DB_USER = "",
      DB_PASS = "",
      DB_NAME = "";

\Amp\run(function() {
	/* If you want ssl, pass as second argument an array with ssl options (an empty options array is valid too); if null is passed, ssl is not enabled either */

	$db = new \Mysql\Pool("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=".DB_NAME);

	/* use an alternative charset... Default is utf8mb4_general_ci */
	$db->setCharset("latin1_general_ci");

	/* do something with your connection(s) maintained by Pool */

	$db->close();
});