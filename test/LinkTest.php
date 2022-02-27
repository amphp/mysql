<?php

namespace Amp\Mysql\Test;

use Amp\Mysql\DataTypes;
use Amp\Mysql\Link;
use Amp\Mysql\Result;
use Amp\PHPUnit\AsyncTestCase;
use Amp\Sql\QueryError;

abstract class LinkTest extends AsyncTestCase
{
    /**
     * Returns the Link class to be tested.
     */
    abstract protected function getLink(string $connectionString): Link;

    public function testQuery()
    {
        $db = $this->getLink("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=test");

        $resultset = $db->execute("SELECT ? AS a", [M_PI]);
        $this->assertInstanceOf(Result::class, $resultset);

        $i = 0;
        foreach ($resultset as $row) {
            $this->assertSame(["a" => M_PI], $row);
            ++$i;
        }

        $this->assertSame(1, $i);
    }

    public function testQueryFetchRow()
    {
        $db = $this->getLink("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=test");

        $resultset = $db->query('SELECT a FROM main WHERE a < 4');
        $this->assertInstanceOf(Result::class, $resultset);

        $this->assertSame(1, $resultset->getColumnCount());

        $got = [];
        foreach ($resultset as $row) {
            $got[] = \array_values($row);
        }

        $this->assertSame($got, [[1], [2], [3]]);
    }

    public function testQueryWithInvalidQuery()
    {
        $this->expectException(QueryError::class);
        $this->expectExceptionMessage('You have an error in your SQL syntax');

        $db = $this->getLink("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=test");

        $db->query("SELECT & FROM main WHERE a = 1");
    }

    public function testMultiStmt()
    {
        $db = $this->getLink("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=test;useCompression=true");

        $resultset = $db->query("SELECT a FROM main; SELECT b FROM main WHERE a = 5; SELECT b AS d, a + 1 AS c FROM main WHERE b > 4");
        $this->assertInstanceOf(Result::class, $resultset);

        $got = [];
        foreach ($resultset as $row) {
            $got[] = \array_values($row);
        }
        $this->assertSame([[1], [2], [3], [4], [5]], $got);
        $this->assertInstanceOf(Result::class, $resultset = $resultset->getNextResult());

        $got = [];
        foreach ($resultset as $row) {
            $got[] = \array_values($row);
        }
        $this->assertSame([[6]], $got);
        $this->assertInstanceOf(Result::class, $resultset = $resultset->getNextResult());

        $fields = $resultset->getFields();

        $got = [];
        foreach ($resultset as $row) {
            $got[] = $row;
        }
        $this->assertSame([["d" => 5, "c" => 5], ["d" => 6, "c" => 6]], $got);

        $this->assertCount(2, $fields);
        $this->assertSame($fields[0]["original_name"], "b");
        $this->assertSame($fields[0]["name"], "d");
        $this->assertSame($fields[0]["type"], DataTypes::MYSQL_TYPE_LONG);
        $this->assertSame($fields[1]["name"], "c");
        $this->assertSame($fields[1]["type"], DataTypes::MYSQL_TYPE_LONGLONG);

        $this->assertNull($resultset->getNextResult());
    }

    public function testPrepared()
    {
        $db = $this->getLink("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=test;useCompression=true");

        $stmt = $db->prepare("SELECT * FROM main WHERE a = ? OR b = :num");
        $base = [
            "catalog" => "def",
            "schema" => "test",
            "table" => "main",
            "original_table" => "main",
            "charset" => 63,
            "columnlen" => 11,
            "type" => 3,
            "flags" => 0,
            "decimals" => 0,
        ];
        $this->assertEquals($stmt->getFields(), [
            \array_merge($base, ["name" => "id", "original_name" => "id", "flags" => 16899]),
            \array_merge($base, ["name" => "a", "original_name" => "a"]),
            \array_merge($base + ["name" => "b", "original_name" => "b"]),
        ]);
        $stmt->bind("num", 5);
        $result = $stmt->execute([2]);
        $this->assertInstanceOf(Result::class, $result);
        $this->assertSame(3, $result->getColumnCount());
        $got = [];
        foreach ($result as $row) {
            $got[] = \array_values($row);
        }
        $this->assertCount(2, $got);

        $stmt = $db->prepare("SELECT * FROM main WHERE a = ? OR b = ?");
        $result = $stmt->execute([1, 8]);
        $this->assertInstanceOf(Result::class, $result);
        $this->assertSame(3, $result->getColumnCount());
        $got = [];
        foreach ($result as $row) {
            $got[] = \array_values($row);
        }
        $this->assertCount(1, $got);

        $stmt = $db->prepare("SELECT * FROM main WHERE a = :a OR b = ?");
        $result = $stmt->execute(["a" => 2, 5]);
        $this->assertInstanceOf(Result::class, $result);
        $this->assertSame(3, $result->getColumnCount());
        $got = [];
        foreach ($result as $row) {
            $got[] = \array_values($row);
        }
        $this->assertCount(2, $got);

        $stmt = $db->prepare("INSERT INTO main (a, b) VALUES (:a, :b)");
        $result = $stmt->execute(["a" => 10, "b" => 11]);
        $this->assertNull($result->getColumnCount());
        $this->assertInstanceOf(Result::class, $result);
        $this->assertGreaterThan(5, $result->getLastInsertId());

        $stmt = $db->prepare("DELETE FROM main WHERE a = :a");
        $result = $stmt->execute(["a" => 10]);
        $this->assertNull($result->getColumnCount());
        $this->assertInstanceOf(Result::class, $result);
    }

    public function testPrepareWithInvalidQuery()
    {
        $this->expectException(QueryError::class);
        $this->expectExceptionMessage('You have an error in your SQL syntax');

        $db = $this->getLink("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=test");

        $statement = $db->prepare("SELECT & FROM main WHERE a = ?");

        $statement->execute(); // Some implementations do not throw until execute() is called.
    }

    public function testBindWithInvalidParamId()
    {
        $this->expectException(\Error::class);
        $this->expectExceptionMessage('Parameter 1 is not defined for this prepared statement');

        $db = $this->getLink("host=" . DB_HOST . ";user=" . DB_USER . ";pass=" . DB_PASS . ";db=test");

        $statement = $db->prepare("SELECT * FROM main WHERE a = ?");

        $statement->bind(1, 1);

        $statement->execute(); // Some implementations do not throw until execute() is called.
    }

    public function testBindWithInvalidParamName()
    {
        $this->expectException(\Error::class);
        $this->expectExceptionMessage('Parameter :b is not defined for this prepared statement');

        $db = $this->getLink("host=" . DB_HOST . ";user=" . DB_USER . ";pass=" . DB_PASS . ";db=test");

        $statement = $db->prepare("SELECT * FROM main WHERE a = :a");

        $statement->bind("b", 1);

        $statement->execute(); // Some implementations do not throw until execute() is called.
    }

    public function testStatementExecuteWithTooFewParams()
    {
        $this->expectException(\Error::class);
        $this->expectExceptionMessage('Parameter 1 for prepared statement missing');

        $db = $this->getLink("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=test");

        $stmt = $db->prepare("SELECT * FROM main WHERE a = ? AND b = ?");
        $stmt->execute([1]);
    }

    public function testExecute()
    {
        $db = $this->getLink("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=test");

        $result = $db->execute("SELECT * FROM test.main WHERE a = ? OR b = ?", [2, 5]);
        $this->assertInstanceOf(Result::class, $result);
        $got = [];
        foreach ($result as $row) {
            $got[] = \array_values($row);
        }
        $this->assertCount(2, $got);
        $this->assertSame([[2, 2, 3], [4, 4, 5]], $got);

        $result = $db->execute("INSERT INTO main (a, b) VALUES (:a, :b)", ["a" => 10, "b" => 11]);
        $this->assertInstanceOf(Result::class, $result);
        $this->assertGreaterThan(5, $result->getLastInsertId());

        $result = $db->execute("DELETE FROM main WHERE a = :a", ["a" => 10]);
        $this->assertInstanceOf(Result::class, $result);
    }

    public function testExecuteWithInvalidQuery()
    {
        $this->expectException(QueryError::class);
        $this->expectExceptionMessage('You have an error in your SQL syntax');

        $db = $this->getLink("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=test");

        $db->execute("SELECT & FROM main WHERE a = ?", [1]);

        $db->close();
    }

    public function testExecuteWithTooFewParams()
    {
        $this->expectException(\Error::class);
        $this->expectExceptionMessage('Parameter 1 for prepared statement missing');

        $db = $this->getLink("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=test");

        $db->execute("SELECT * FROM main WHERE a = ? AND b = ?", [1]);

        $db->close();
    }

    public function testPreparedWithNegativeValue()
    {
        $db = $this->getLink("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=test");

        $db->query("DROP TABLE IF EXISTS tmp");

        $stmt = $db->prepare("CREATE TABLE tmp SELECT ? AS a");
        $stmt->execute([-1]);

        $stmt = $db->prepare("SELECT a FROM tmp");
        $result = $stmt->execute();
        $result = \iterator_to_array($result);

        $this->assertEquals(\array_values(\array_shift($result)), [-1]);

        $db->close();
    }

    public function testTransaction()
    {
        $db = $this->getLink("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=test");

        $transaction = $db->beginTransaction();

        $statement = $transaction->prepare("INSERT INTO main (a, b) VALUES (?, ?)");
        $result = $statement->execute([6, 7]);
        $this->assertInstanceOf(Result::class, $result);
        $this->assertGreaterThan(5, $result->getLastInsertId());

        $result = $transaction->execute("SELECT * FROM main WHERE a = ?", [6]);

        $got = [];
        foreach ($result as $row) {
            $got[] = \array_values($row);
        }
        $this->assertCount(1, $got);
        $result = $result->getNextResult();

        $transaction->rollback();

        $result = $db->execute("SELECT * FROM main WHERE a = ?", [6]);

        $got = [];
        foreach ($result as $row) {
            $got[] = \array_values($row);
        }
        $this->assertCount(0, $got);

        $db->close();
    }

    /**
     * @depends testTransaction
     */
    public function testInsertSelect()
    {
        $db = $this->getLink("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=test");

        $a = 1;

        $transaction = $db->beginTransaction();

        try {
            $statement = $transaction->prepare("SELECT a, b FROM main WHERE a >= ?");

            $count = \count(\iterator_to_array($statement->execute([$a])));

            $statement = $transaction->prepare("INSERT INTO main (a, b) SELECT a, b FROM main WHERE a >= ?");

            $result = $statement->execute([$a]);

            $this->assertSame($count, $result->getRowCount());
            $this->assertGreaterThan(5, $result->getLastInsertId());
        } finally {
            $transaction->rollback();
        }

        $db->close();
    }

    public function testJsonDecoding()
    {
        $db = $this->getLink("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=test");

        $result = $db->execute("SELECT a FROM test.json");

        foreach ($result as $row) {
            $this->assertSame(["a" => '{"key": "value"}'], $row);
        }
    }
}
