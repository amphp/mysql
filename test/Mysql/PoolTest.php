<?php

use Amp\NativeReactor;
use Amp\Mysql\Pool;

class PoolTest extends \PHPUnit_Framework_TestCase {
	function testConnect() {
		$complete = false;
		(new NativeReactor)->run(function($reactor) use (&$complete) {
			$db = new Pool("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=connectiontest", null, $reactor);
			yield $db->init(); // force waiting for connection
			/* use an alternative charset... Default is utf8mb4_general_ci */
			$db->setCharset("latin1_general_ci");

			$db->close();
			$complete = true;
		});
		$this->assertEquals(true, $complete, "Database commands did not complete.");
	}

	/** This should throw an exception as the password is incorrect. */
	function testWrongPassword() {
		$this->setExpectedException("Exception");
		(new NativeReactor)->run(function($reactor) {
			$db = new Pool("host=".DB_HOST.";user=".DB_USER.";pass=the_wrong_password;db=connectiontest", null, $reactor);

			/* Try a query */
			yield $db->query("CREATE TABLE tmp SELECT 1 AS a, 2 AS b");
			$db->close();
		});
	}

	/* common test for all the Pool functions which are just a thin wrapper for the Connection class */
	function testVirtualConnection() {
		$complete = false;
		(new NativeReactor)->run(function($reactor) use (&$complete) {
			$db = new Pool("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=connectiontest", null, $reactor);

			/* Multiple queries one after the other must be hold back and dispatched to new connections */
			for ($i = 0; $i < 5; $i++) {
				$pings[] = $db->ping();
			}

			yield \Amp\all($pings);
			$db->close();
			$complete = true;
		});
		$this->assertEquals(true, $complete, "Database commands did not complete.");
	}
}