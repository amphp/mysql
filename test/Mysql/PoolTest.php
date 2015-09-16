<?php

use Amp\Mysql\Pool;
use Amp\NativeReactor;

class PoolTest extends \PHPUnit_Framework_TestCase {
	function testConnect() {
		$complete = false;
		\Amp\reactor(\Amp\driver());
		\Amp\run(function() use (&$complete) {
			$db = new Pool("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=connectiontest");
			yield $db->init(); // force waiting for connection
			/* use an alternative charset... Default is utf8mb4_general_ci */
			$db->setCharset("latin1_general_ci");

			$complete = true;
		});
		$this->assertEquals(true, $complete, "Database commands did not complete.");
	}

	/** This should throw an exception as the password is incorrect. */
	function testWrongPassword() {
		$this->setExpectedException("Exception");
		\Amp\reactor(\Amp\driver());
		\Amp\run(function() {
			$db = new Pool("host=".DB_HOST.";user=".DB_USER.";pass=the_wrong_password;db=connectiontest");

			/* Try a query */
			yield $db->query("CREATE TABLE tmp SELECT 1 AS a, 2 AS b");
		});
	}

	/* common test for all the Pool functions which are just a thin wrapper for the Connection class */
	function testVirtualConnection() {
		$complete = false;
		\Amp\reactor(\Amp\driver());
		\Amp\run(function() use (&$complete) {
			$db = new Pool("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=connectiontest");

			/* Multiple queries one after the other must be hold back and dispatched to new connections */
			for ($i = 0; $i < 5; $i++) {
				$pings[] = $db->ping();
			}

			yield \Amp\all($pings);
			$complete = true;
		});
		$this->assertEquals(true, $complete, "Database commands did not complete.");
	}
}