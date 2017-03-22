<?php

use Amp\Mysql\Connection;
use Amp\Mysql\DataTypes;
use PHPUnit\Framework\TestCase;

class ConnectionTest extends TestCase {
	function testConnect() {
		$complete = false;
		\Amp\Loop::run(function() use (&$complete) {
			$db = new Connection("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=connectiontest");
			yield $db->connect();

			/* use an alternative charset... Default is utf8mb4_general_ci */
			$db->setCharset("latin1_general_ci");

			$db->close();
			$complete = true;
		});
		$this->assertEquals(true, $complete, "Database commands did not complete.");
	}

	function testQuery() {
		\Amp\Loop::run(function() {
			$db = new Connection("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=connectiontest");
			$db->connect();

			$resultset = yield $db->query("SELECT 1 AS a");
			$this->assertEquals(yield $resultset->rowCount(), 1);
			$this->assertEquals(yield $resultset->fetch(), ["a" => 1, 0 => 1]);
			$resultset = yield $db->query("SELECT 1 AS a");
			$this->assertEquals(yield $resultset->fetchRow(), [0 => 1]);
			$resultset = yield $db->query("SELECT 1 AS a");
			$this->assertEquals(yield $resultset->fetchObject(), (object) ["a" => 1]);

			$this->assertEquals(yield $resultset->fetchAll(), [["a" => 1, 0 => 1]]);
			$this->assertEquals(yield $resultset->fetchRows(), [[0 => 1]]);
			$this->assertEquals(yield $resultset->fetchObjects(), [(object) ["a" => 1]]);
		});
	}

	function testQueryFetchRow() {
		\Amp\Loop::run(function () {
			$db = new Connection("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=connectiontest");
			$db->connect();

			$db->query('DROP TABLE tmp');
			$db->query('CREATE TABLE tmp (a int)');
			$db->query('INSERT INTO tmp VALUES (1), (2), (3)');

			$resultset = yield $db->query('SELECT a FROM tmp');

			$got = [];

			while ($row = yield $resultset->fetchRow()) {
				$got[] = $row;
			}

			$this->assertEquals($got, [[1], [2], [3]]);
		});
	}
	
	function testMultiStmt() {
		\Amp\Loop::run(function() {
			$db = new Connection("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=connectiontest");
			$db->connect();

			$db->query("CREATE DATABASE IF NOT EXISTS alt");
			$db->useDb("alt");

			$db->query("DROP TABLE tmp"); // just in case it would exist...
			$db->query("CREATE TABLE tmp SELECT 1 AS a, 2 AS b");
			$db->query("INSERT INTO tmp VALUES (5, 6), (8, 9)");

			$resultset = yield $db->query("SELECT a FROM tmp; SELECT b FROM tmp WHERE a = 5; SELECT b AS d, a + 1 AS c FROM tmp WHERE b < 7");
			$this->assertEquals(yield $resultset->rowCount(), 3);

			$resultset = (yield $resultset->next());
			$this->assertEquals(yield $resultset->fetchRow(), [6]);

			$resultset = yield $resultset->next();
			$fields = yield $resultset->getFields();
			$this->assertEquals(count($fields), 2);
			$this->assertEquals($fields[0]["original_name"], "b");
			$this->assertEquals($fields[0]["name"], "d");
			$this->assertEquals($fields[0]["type"], DataTypes::MYSQL_TYPE_LONG);
			$this->assertEquals($fields[1]["name"], "c");
			$this->assertEquals($fields[1]["type"], DataTypes::MYSQL_TYPE_LONGLONG);

			yield $db->query("DROP DATABASE alt");
		});
	}

	function testPrepared() {
		\Amp\Loop::run(function() {
			$db = new Connection("host=" . DB_HOST . ";user=" . DB_USER . ";pass=" . DB_PASS . ";db=connectiontest");
			$db->connect();

			$db->query("CREATE TEMPORARY TABLE tmp SELECT 1 AS a, 2 AS b");
			$db->query("INSERT INTO tmp VALUES (5, 6), (8, 9), (10, 11), (12, 13)");

			$stmt = yield $db->prepare("SELECT * FROM tmp WHERE a = ? OR b = :num");
			$base = [
				"catalog" => "def",
				"schema" => "connectiontest",
				"table" => "tmp",
				"original_table" => "tmp",
				"charset" => 63,
				"columnlen" => 1,
				"type" => 3,
				"flags" => 1,
				"decimals" => 0,
			];
			$this->assertEquals(yield $stmt->getFields(), [$base + ["name" => "a", "original_name" => "a"], $base + ["name" => "b", "original_name" => "b"]]);
			$result = (yield $stmt->execute([5, "num" => 9]));
			$this->assertEquals(yield $result->rowCount(), 2);

			$result = yield $db->prepare("SELECT * FROM tmp WHERE a = ? OR b = ?", [5, 8]);
			$this->assertEquals((yield $result->rowCount()), 1);

			$result = yield $db->prepare("SELECT * FROM tmp WHERE a = :a OR b = ? OR a = :a", ["a" => [5, 10], 9]);
			$this->assertEquals((yield $result->rowCount()), 3);

			$stmt = yield $db->prepare("INSERT INTO tmp VALUES (:foo, :bar)");
			$stmt->bind("foo", 5);
			$result = yield $stmt->execute(["bar" => 9]);
			$this->assertEquals($result->affectedRows, 1);
		});
	}

	function testPreparedWithNegativeValue() {
		\Amp\Loop::run(function() {
			$db = new Connection("host=" . DB_HOST . ";user=" . DB_USER . ";pass=" . DB_PASS . ";db=connectiontest");
			$db->connect();

			$db->query("DROP TABLE tmp"); // just in case it would exist...
			yield $db->prepare("CREATE TABLE tmp SELECT ? AS a", [-1]);
			$result = yield $db->prepare("SELECT a FROM tmp", []);
			$this->assertEquals(yield $result->fetchRow(), [-1]);
		});
	}
}
