<?php

class ConnectionTest extends \PHPUnit_Framework_TestCase {
	function testConnect() {
		$complete = false;
		(new \Amp\NativeReactor)->run(function($reactor) use (&$complete) {
			$db = new \Mysql\Connection("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=ConnectionTest", null, $reactor);
			yield $db->connect();

			/* use an alternative charset... Default is utf8mb4_general_ci */
			$db->setCharset("latin1_general_ci");

			$db->close();
			$complete = true;
		});
		$this->assertEquals(true, $complete, "Database commands did not complete.");
	}

	function testQuery() {
		(new \Amp\NativeReactor)->run(function($reactor) {
			$db = new \Mysql\Connection("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=ConnectionTest", null, $reactor);
			$db->connect();

			$resultset = (yield $db->query("SELECT 1 AS a"));
			$this->assertEquals((yield $resultset->rowCount()), 1);
			$this->assertEquals((yield $resultset->fetch()), ["a" => 1, 0 => 1]);
			$resultset = (yield $db->query("SELECT 1 AS a"));
			$this->assertEquals((yield $resultset->fetchRow()), [0 => 1]);
			$resultset = (yield $db->query("SELECT 1 AS a"));
			$this->assertEquals((yield $resultset->fetchObject()), (object) ["a" => 1]);

			$this->assertEquals((yield $resultset->fetchAll()), [["a" => 1, 0 => 1]]);
			$this->assertEquals((yield $resultset->fetchRows()), [[0 => 1]]);
			$this->assertEquals((yield $resultset->fetchObjects()), [(object) ["a" => 1]]);

			$db->close();
		});
	}

	function testMultiStmt() {
		(new \Amp\NativeReactor)->run(function($reactor) {
			$db = new \Mysql\Connection("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=ConnectionTest", null, $reactor);
			$db->connect();

			$db->query("CREATE DATABASE IF NOT EXISTS alt");
			$db->useDb("alt");

			$db->query("DROP TABLE tmp"); // just in case it would exist...
			$db->query("CREATE TABLE tmp SELECT 1 AS a, 2 AS b");
			$db->query("INSERT INTO tmp VALUES (5, 6), (8, 9)");

			$resultset = (yield $db->query("SELECT a FROM tmp; SELECT b FROM tmp WHERE a = 5; SELECT b AS d, SUM(a) AS c FROM tmp WHERE b < 7"));
			$this->assertEquals((yield $resultset->rowCount()), 3);

			$resultset = (yield $resultset->next());
			$this->assertEquals((yield $resultset->fetchRow()), [6]);

			$resultset = (yield $resultset->next());
			$fields = (yield $resultset->getFields());
			$this->assertEquals(count($fields), 2);
			$this->assertEquals($fields[0]["original_name"], "b");
			$this->assertEquals($fields[0]["name"], "d");
			$this->assertEquals($fields[0]["type"], \Mysql\DataTypes::MYSQL_TYPE_LONG);
			$this->assertEquals($fields[1]["name"], "c");
			$this->assertEquals($fields[1]["type"], \Mysql\DataTypes::MYSQL_TYPE_NEWDECIMAL);

			yield $db->query("DROP DATABASE alt");
			$db->close();
		});
	}
}