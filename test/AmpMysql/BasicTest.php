<?php

class BasicTest extends \PHPUnit_Framework_TestCase {

	function setup() {
		//Done before each test.
	}

	function teardown() {
		//Done after each test.
	}

	function testConnect() {
		$complete = false;
		$callable = function() use (&$complete)  {
			/* If you want ssl, pass as second argument an array with ssl options (an empty options array is valid too); if null is passed, ssl is not enabled either */

			$db = new \Mysql\Pool("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=".DB_NAME);

			/* use an alternative charset... Default is utf8mb4_general_ci */
			$db->setCharset("latin1_general_ci");

			/* do something with your connection(s) maintained by Pool */
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
			$db = new \Mysql\Pool("host=".DB_HOST.";user=".DB_USER.";pass=12345;db=".DB_NAME);

			/* Create table and insert a few rows */
			/* we need to wait until table is finished, so that we can insert. */
			yield $db->query("CREATE TABLE tmp SELECT 1 AS a, 2 AS b");

			$promises = [];
			foreach (range(1, 5) as $num) {
				$promises[] = $db->query("INSERT INTO tmp (a, b) VALUES ($num, $num * 2)");
			}

			/* wait until everything is inserted (in case where we wouldn't have to wait, we also could just  */
			yield $promises;

			$complete = true;
		};

		\Amp\run($callable);
	}
}




