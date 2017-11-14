<?php

use Amp\Mysql\CommandResult;
use Amp\Mysql\DataTypes;
use Amp\Mysql\ResultSet;
use PHPUnit\Framework\TestCase;
use function Amp\Mysql\connect;

class ConnectionTest extends TestCase {
    public function testConnect() {
        $complete = false;
        \Amp\Loop::run(function () use (&$complete) {
            /** @var \Amp\Mysql\Connection $db */
            $db = yield connect("host=".DB_HOST." user=".DB_USER." pass=".DB_PASS." db=connectiontest");

            /* use an alternative charset... Default is utf8mb4_general_ci */
            yield $db->setCharset("latin1_general_ci");

            $db->close();
            $complete = true;
        });
        $this->assertTrue($complete, "Database commands did not complete.");
    }

    public function testQuery() {
        \Amp\Loop::run(function () {
            /** @var \Amp\Mysql\Connection $db */
            $db = yield connect("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=connectiontest");

            /** @var \Amp\Mysql\ResultSet $resultset */
            $resultset = yield $db->query("SELECT 1 AS a");
            $this->assertInstanceOf(ResultSet::class, $resultset);

            for ($i = 0; yield $resultset->advance(); ++$i) {
                $this->assertEquals(["a" => 1], $resultset->getCurrent());
            }

            $this->assertSame(1, $i);
        });
    }

    public function testQueryFetchRow() {
        \Amp\Loop::run(function () {
            /** @var \Amp\Mysql\Connection $db */
            $db = yield connect("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=connectiontest");

            $db->query('DROP TABLE tmp');
            $db->query('CREATE TABLE tmp (a int)');
            $db->query('INSERT INTO tmp VALUES (1), (2), (3)');

            /** @var \Amp\Mysql\ResultSet $resultset */
            $resultset = yield $db->query('SELECT a FROM tmp');
            $this->assertInstanceOf(ResultSet::class, $resultset);

            $got = [];
            while (yield $resultset->advance(ResultSet::FETCH_ARRAY)) {
                $got[] = $resultset->getCurrent();
            }

            $this->assertEquals($got, [[1], [2], [3]]);
        });
    }

    public function testMultiStmt() {
        \Amp\Loop::run(function () {
            /** @var \Amp\Mysql\Connection $db */
            $db = yield connect("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=connectiontest");

            $db->query("CREATE DATABASE IF NOT EXISTS alt");
            $db->useDb("alt");

            $db->query("DROP TABLE tmp"); // just in case it would exist...
            $db->query("CREATE TABLE tmp SELECT 1 AS a, 2 AS b");

            /** @var \Amp\Mysql\ResultSet $resultset */
            $resultset = yield $db->query("INSERT INTO tmp VALUES (5, 6), (8, 9); SELECT a FROM tmp; SELECT b FROM tmp WHERE a = 5; SELECT b AS d, a + 1 AS c FROM tmp WHERE b < 7");
            $this->assertInstanceOf(ResultSet::class, $resultset);

            $got = [];
            while (yield $resultset->advance(ResultSet::FETCH_ARRAY)) {
                $got[] = $resultset->getCurrent();
            }

            $this->assertEquals($got, [[1], [5], [8], [6], [2, 2], [6, 6]]);

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

    public function testPrepared() {
        \Amp\Loop::run(function () {
            /** @var \Amp\Mysql\Connection $db */
            $db = yield connect("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=connectiontest");

            $db->query("CREATE TEMPORARY TABLE tmp SELECT 1 AS a, 2 AS b");
            $db->query("INSERT INTO tmp VALUES (5, 6), (8, 9), (10, 11), (12, 13)");

            /**
             * @var \Amp\Mysql\Statement $stmt
             * @var \Amp\Mysql\ResultSet $result
             */
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
            $stmt->bind("num", 9);
            $result = yield $stmt->execute(5);
            $this->assertInstanceOf(ResultSet::class, $result);
            $got = [];
            while (yield $result->advance(ResultSet::FETCH_ARRAY)) {
                $got[] = $result->getCurrent();
            }
            $this->assertCount(2, $got);

            /** @var \Amp\Mysql\Statement $stmt */
            $stmt = yield $db->prepare("SELECT * FROM tmp WHERE a = ? OR b = ?");
            $result = yield $stmt->execute(5, 8);
            $this->assertInstanceOf(ResultSet::class, $result);
            $got = [];
            while (yield $result->advance(ResultSet::FETCH_ARRAY)) {
                $got[] = $result->getCurrent();
            }
            $this->assertCount(1, $got);

            /** @var \Amp\Mysql\Statement $stmt */
            $stmt = yield $db->prepare("SELECT * FROM tmp WHERE a = :a OR b = ?");
            $stmt->bind("a", 5);
            $result = yield $stmt->execute(9);
            $this->assertInstanceOf(ResultSet::class, $result);
            $got = [];
            while (yield $result->advance(ResultSet::FETCH_ARRAY)) {
                $got[] = $result->getCurrent();
            }
            $this->assertCount(2, $got);

            $stmt = yield $db->prepare("INSERT INTO tmp VALUES (:foo, :bar)");
            $stmt->bind("foo", 5);
            $stmt->bind("bar", 9);
            /** @var \Amp\Mysql\CommandResult $result */
            $result = yield $stmt->execute();
            $this->assertInstanceOf(CommandResult::class, $result);
            $this->assertSame(1, $result->affectedRows());
        });
    }

    public function testExecute() {
        \Amp\Loop::run(function () {
            /** @var \Amp\Mysql\Connection $db */
            $db = yield connect("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=connectiontest");

            $db->query("CREATE TEMPORARY TABLE tmp SELECT 1 AS a, 2 AS b");
            $db->query("INSERT INTO tmp VALUES (5, 6), (8, 9), (10, 11), (12, 13)");

            /** @var \Amp\Mysql\ResultSet $result */
            $result = yield $db->execute("SELECT * FROM tmp WHERE a = ? OR b = ?", 5, 9);
            $this->assertInstanceOf(ResultSet::class, $result);
            $got = [];
            while (yield $result->advance(ResultSet::FETCH_ARRAY)) {
                $got[] = $result->getCurrent();
            }
            $this->assertCount(2, $got);
            $this->assertSame([[5, 6], [8, 9]], $got);

            /** @var \Amp\Mysql\CommandResult $result */
            $result = yield $db->execute("INSERT INTO tmp VALUES (?, ?)", 14, 15);
            $this->assertInstanceOf(CommandResult::class, $result);
            $this->assertSame(1, $result->affectedRows());
        });
    }

    public function testPreparedWithNegativeValue() {
        \Amp\Loop::run(function () {
            /** @var \Amp\Mysql\Connection $db */
            $db = yield connect("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=connectiontest");

            /** @var \Amp\Mysql\Statement $stmt */
            $db->query("DROP TABLE tmp"); // just in case it would exist...
            $stmt = yield $db->prepare("CREATE TABLE tmp SELECT ? AS a");
            yield $stmt->execute(-1);

            /** @var \Amp\Mysql\ResultSet $result */
            $stmt = yield $db->prepare("SELECT a FROM tmp");
            $result = yield $stmt->execute();
            yield $result->advance(ResultSet::FETCH_ARRAY);

            $this->assertEquals($result->getCurrent(), [-1]);
        });
    }
}
