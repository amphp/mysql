<?php

namespace Amp\Mysql\Test;

use Amp\Mysql\MysqlColumnDefinition;
use Amp\Mysql\MysqlConfig;
use Amp\Mysql\MysqlDataType;
use Amp\Mysql\MysqlLink;
use Amp\Mysql\MysqlResult;
use Amp\PHPUnit\AsyncTestCase;
use Amp\Sql\QueryError;

abstract class LinkTest extends AsyncTestCase
{
    /**
     * Returns the Link class to be tested.
     */
    abstract protected function getLink(bool $useCompression = false): MysqlLink;

    protected function getConfig(bool $useCompression = false): MysqlConfig
    {
        $config = MysqlConfig::fromAuthority(DB_HOST, DB_USER, DB_PASS, 'test');
        if ($useCompression) {
            $config = $config->withCompression();
        }

        return $config;
    }

    public function testQuery()
    {
        $db = $this->getLink();

        $resultset = $db->execute("SELECT ? AS a", [M_PI]);
        $this->assertInstanceOf(MysqlResult::class, $resultset);

        $i = 0;
        foreach ($resultset as $row) {
            $this->assertSame(["a" => M_PI], $row);
            ++$i;
        }

        $this->assertSame(1, $i);
    }

    public function testQueryFetchRow()
    {
        $db = $this->getLink();

        $resultset = $db->query('SELECT a FROM main WHERE a < 4');
        $this->assertInstanceOf(MysqlResult::class, $resultset);

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

        $db = $this->getLink();

        $db->query("SELECT & FROM main WHERE a = 1");
    }

    public function testMultiStmt()
    {
        $db = $this->getLink(true);

        $resultset = $db->query("SELECT a FROM main; SELECT b FROM main WHERE a = 5; SELECT b AS d, a + 1 AS c FROM main WHERE b > 4");
        $this->assertInstanceOf(MysqlResult::class, $resultset);

        $got = [];
        foreach ($resultset as $row) {
            $got[] = \array_values($row);
        }
        $this->assertSame([[1], [2], [3], [4], [5]], $got);
        $this->assertInstanceOf(MysqlResult::class, $resultset = $resultset->getNextResult());

        $got = [];
        foreach ($resultset as $row) {
            $got[] = \array_values($row);
        }
        $this->assertSame([[6]], $got);
        $this->assertInstanceOf(MysqlResult::class, $resultset = $resultset->getNextResult());

        $fields = $resultset->getColumnDefinitions();

        $got = [];
        foreach ($resultset as $row) {
            $got[] = $row;
        }
        $this->assertSame([["d" => 5, "c" => 5], ["d" => 6, "c" => 6]], $got);

        $this->assertCount(2, $fields);
        $this->assertSame($fields[0]->originalName, "b");
        $this->assertSame($fields[0]->name, "d");
        $this->assertSame($fields[0]->type, MysqlDataType::Long);
        $this->assertSame($fields[1]->name, "c");
        $this->assertSame($fields[1]->type, MysqlDataType::LongLong);

        $this->assertNull($resultset->getNextResult());
    }

    public function testPrepared()
    {
        $db = $this->getLink(true);

        $stmt = $db->prepare("SELECT id as no, a, b FROM main as table_alias WHERE a = ? OR b = :num");
        $base = [
            "catalog" => "def",
            "schema" => "test",
            "table" => "table_alias",
            "originalTable" => "main",
            "charset" => 63,
            "length" => 11,
            "type" => MysqlDataType::from(3),
            "flags" => 0,
            "decimals" => 0,
        ];

        $this->assertEquals($stmt->getColumnDefinitions(), [
            new MysqlColumnDefinition(...\array_merge($base, ["name" => "no", "originalName" => "id", "flags" => 16899])),
            new MysqlColumnDefinition(...\array_merge($base, ["name" => "a", "originalName" => "a"])),
            new MysqlColumnDefinition(...\array_merge($base + ["name" => "b", "originalName" => "b"])),
        ]);

        $stmt->bind("num", 5);
        $result = $stmt->execute([2]);
        $this->assertInstanceOf(MysqlResult::class, $result);
        $this->assertSame(3, $result->getColumnCount());
        $got = [];
        foreach ($result as $row) {
            $got[] = \array_values($row);
        }
        $this->assertCount(2, $got);

        $stmt = $db->prepare("SELECT * FROM main WHERE a = ? OR b = ?");
        $result = $stmt->execute([1, 8]);
        $this->assertInstanceOf(MysqlResult::class, $result);
        $this->assertSame(3, $result->getColumnCount());
        $got = [];
        foreach ($result as $row) {
            $got[] = \array_values($row);
        }
        $this->assertCount(1, $got);

        $stmt = $db->prepare("SELECT * FROM main WHERE a = :a OR b = ?");
        $result = $stmt->execute(["a" => 2, 5]);
        $this->assertInstanceOf(MysqlResult::class, $result);
        $this->assertSame(3, $result->getColumnCount());
        $got = [];
        foreach ($result as $row) {
            $got[] = \array_values($row);
        }
        $this->assertCount(2, $got);

        $stmt = $db->prepare("INSERT INTO main (a, b) VALUES (:a, :b)");
        $result = $stmt->execute(["a" => 10, "b" => 11]);
        $this->assertNull($result->getColumnCount());
        $this->assertInstanceOf(MysqlResult::class, $result);
        $this->assertGreaterThan(5, $result->getLastInsertId());

        $stmt = $db->prepare("DELETE FROM main WHERE a = :a");
        $result = $stmt->execute(["a" => 10]);
        $this->assertNull($result->getColumnCount());
        $this->assertInstanceOf(MysqlResult::class, $result);
    }

    public function testPrepareWithInvalidQuery()
    {
        $this->expectException(QueryError::class);
        $this->expectExceptionMessage('You have an error in your SQL syntax');

        $db = $this->getLink();

        $statement = $db->prepare("SELECT & FROM main WHERE a = ?");

        $statement->execute(); // Some implementations do not throw until execute() is called.
    }

    public function testBindWithInvalidParamId()
    {
        $this->expectException(\Error::class);
        $this->expectExceptionMessage('Parameter 1 is not defined for this prepared statement');

        $db = $this->getLink();

        $statement = $db->prepare("SELECT * FROM main WHERE a = ?");

        $statement->bind(1, 1);

        $statement->execute(); // Some implementations do not throw until execute() is called.
    }

    public function testBindWithInvalidParamName()
    {
        $this->expectException(\Error::class);
        $this->expectExceptionMessage('Parameter :b is not defined for this prepared statement');

        $db = $this->getLink();

        $statement = $db->prepare("SELECT * FROM main WHERE a = :a");

        $statement->bind("b", 1);

        $statement->execute(); // Some implementations do not throw until execute() is called.
    }

    public function testStatementExecuteWithTooFewParams()
    {
        $this->expectException(\Error::class);
        $this->expectExceptionMessage('Parameter 1 for prepared statement missing');

        $db = $this->getLink();

        $stmt = $db->prepare("SELECT * FROM main WHERE a = ? AND b = ?");
        $stmt->execute([1]);
    }

    public function testExecute()
    {
        $db = $this->getLink();

        $result = $db->execute("SELECT * FROM test.main WHERE a = ? OR b = ?", [2, 5]);
        $this->assertInstanceOf(MysqlResult::class, $result);
        $got = [];
        foreach ($result as $row) {
            $got[] = \array_values($row);
        }
        $this->assertCount(2, $got);
        $this->assertSame([[2, 2, 3], [4, 4, 5]], $got);

        $result = $db->execute("INSERT INTO main (a, b) VALUES (:a, :b)", ["a" => 10, "b" => 11]);
        $this->assertInstanceOf(MysqlResult::class, $result);
        $this->assertGreaterThan(5, $result->getLastInsertId());

        $result = $db->execute("DELETE FROM main WHERE a = :a", ["a" => 10]);
        $this->assertInstanceOf(MysqlResult::class, $result);
    }

    public function testExecuteWithInvalidQuery()
    {
        $this->expectException(QueryError::class);
        $this->expectExceptionMessage('You have an error in your SQL syntax');

        $db = $this->getLink();

        $db->execute("SELECT & FROM main WHERE a = ?", [1]);

        $db->close();
    }

    public function testExecuteWithTooFewParams()
    {
        $this->expectException(\Error::class);
        $this->expectExceptionMessage('Parameter 1 for prepared statement missing');

        $db = $this->getLink();

        $db->execute("SELECT * FROM main WHERE a = ? AND b = ?", [1]);

        $db->close();
    }

    public function testPreparedWithNegativeValue()
    {
        $db = $this->getLink();

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
        $db = $this->getLink();

        $transaction = $db->beginTransaction();

        $statement = $transaction->prepare("INSERT INTO main (a, b) VALUES (?, ?)");
        $result = $statement->execute([6, 7]);
        $this->assertInstanceOf(MysqlResult::class, $result);
        $this->assertGreaterThan(5, $result->getLastInsertId());

        $result = $transaction->query("SELECT * FROM main WHERE a = 6");

        $got = [];
        foreach ($result as $row) {
            $got[] = \array_values($row);
        }
        $this->assertCount(1, $got);
        $this->assertNull($result->getNextResult());

        $result = $transaction->execute("SELECT * FROM main WHERE a = ?", [6]);

        $got = [];
        foreach ($result as $row) {
            $got[] = \array_values($row);
        }
        $this->assertCount(1, $got);
        $this->assertNull($result->getNextResult());

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
        $db = $this->getLink();

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
        $db = $this->getLink();

        $result = $db->execute("SELECT a FROM test.json");

        foreach ($result as $row) {
            $this->assertSame(["a" => '{"key": "value"}'], $row);
        }
    }
}
