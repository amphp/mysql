<?php

class PoolTest extends \PHPUnit_Framework_TestCase {
	function testConnect() {
		$complete = false;
		$callable = function() use (&$complete)  {
			$db = new \Mysql\Pool("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=ConnectionTest");

			/* use an alternative charset... Default is utf8mb4_general_ci */
			$db->setCharset("latin1_general_ci");

			$db->close();
			$complete = true;
		};

		\Amp\run($callable);
		$this->assertEquals(true, $complete, "Database commands did not complete.");
	}

	/**
	 * This should throw an exception as the password is incorrect.
	 */
	function testWrongPassword() {
		$this->setExpectedException("Exception");
		$callable = function() use (&$complete) {
			$db = new \Mysql\Pool("host=".DB_HOST.";user=".DB_USER.";pass=the_wrong_password;db=ConnectionTest");

			/* Try a query */
			yield $db->query("CREATE TABLE tmp SELECT 1 AS a, 2 AS b");
			$complete = true;
		};

		\Amp\run($callable);
	}
}