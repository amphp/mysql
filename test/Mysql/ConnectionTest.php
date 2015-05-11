<?php

class ConnectionTest extends \PHPUnit_Framework_TestCase {
	function testConnect() {
		$complete = false;
		\Amp\run(function() use (&$complete)  {
			$db = new \Mysql\Connection("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=ConnectionTest");
			$db->connect();

			/* use an alternative charset... Default is utf8mb4_general_ci */
			$db->setCharset("latin1_general_ci");

			$db->close();
			$complete = true;
		});
		$this->assertEquals(true, $complete, "Database commands did not complete.");
	}


}