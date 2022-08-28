<?php

namespace Amp\Mysql\Test;

use Amp\Mysql\MysqlColumnDefinition;
use Amp\Mysql\MysqlDataType;
use Amp\Mysql\MysqlLink;
use Amp\Mysql\MysqlResult;
use Amp\Sql\QueryError;
use Amp\Sql\Result;
use Amp\Sql\SqlException;

abstract class LinkTest extends MysqlTestCase
{
    public const EPOCH = '1970-01-01 00:00:00';

    /**
     * Returns the Link class to be tested.
     */
    abstract protected function getLink(bool $useCompression = false): MysqlLink;

    public function testQuery()
    {
        $db = $this->getLink();

        $result = $db->execute("SELECT ? AS a", [M_PI]);
        $this->assertInstanceOf(MysqlResult::class, $result);

        $i = 0;
        foreach ($result as $row) {
            $this->assertSame(["a" => M_PI], $row);
            ++$i;
        }

        $this->assertSame(1, $i);
    }

    public function testQueryFetchRow()
    {
        $db = $this->getLink();

        $result = $db->query('SELECT a FROM main WHERE a < 4');
        $this->assertInstanceOf(MysqlResult::class, $result);

        $this->assertSame(1, $result->getColumnCount());

        $got = [];
        foreach ($result as $row) {
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

        $result = $db->query("SELECT a FROM main; SELECT b FROM main WHERE a = 5; SELECT b AS d, a + 1 AS c FROM main WHERE b > 4");
        $this->assertInstanceOf(MysqlResult::class, $result);

        $got = [];
        foreach ($result as $row) {
            $got[] = \array_values($row);
        }
        $this->assertSame([[1], [2], [3], [4], [5]], $got);
        $this->assertInstanceOf(MysqlResult::class, $result = $result->getNextResult());

        $got = [];
        foreach ($result as $row) {
            $got[] = \array_values($row);
        }
        $this->assertSame([[6]], $got);
        $this->assertInstanceOf(MysqlResult::class, $result = $result->getNextResult());

        $fields = $result->getColumnDefinitions();

        $got = [];
        foreach ($result as $row) {
            $got[] = $row;
        }
        $this->assertSame([["d" => 5, "c" => 5], ["d" => 6, "c" => 6]], $got);

        $this->assertCount(2, $fields);
        $this->assertSame($fields[0]->originalName, "b");
        $this->assertSame($fields[0]->name, "d");
        $this->assertSame($fields[0]->type, MysqlDataType::Long);
        $this->assertSame($fields[1]->name, "c");
        $this->assertSame($fields[1]->type, MysqlDataType::LongLong);

        $this->assertNull($result->getNextResult());
    }

    public function testNextResultBeforeConsumption()
    {
        $db = $this->getLink(true);

        $result = $db->query("SELECT a FROM main; SELECT b FROM main;");

        $this->expectException(\Error::class);
        $this->expectExceptionMessage('Consume entire current result before requesting next result');

        $result->getNextResult();
    }

    public function testQueryWithUnconsumedTupleResult()
    {
        $db = $this->getLink();

        $result = $db->query("SELECT a FROM main");

        $this->assertInstanceOf(Result::class, $result);

        unset($result); // Force destruction of result object.

        $result = $db->query("SELECT b FROM main");

        $this->assertInstanceOf(Result::class, $result);
    }

    public function testUnconsumedMultiResult()
    {
        $db = $this->getLink(true);

        $result = $db->query("SELECT a FROM main; SELECT b FROM main");

        unset($result);

        $result = $db->query("SELECT a, b FROM main WHERE a = 5");

        $got = [];
        foreach ($result as $row) {
            $got[] = $row;
        }
        self::assertSame([['a' => 5, 'b' => 6]], $got);
    }

    public function testPrepared()
    {
        $db = $this->getLink(true);

        $stmt = $db->prepare("SELECT id AS no, a AS d, b, c FROM main AS table_alias WHERE b = ? AND c = :date AND d = :data");

        $base = [
            "catalog" => "def",
            "schema" => "test",
            "table" => "table_alias",
            "originalTable" => "main",
            "charset" => 63,
            "length" => 11,
            "type" => MysqlDataType::Long,
            "flags" => 0,
            "decimals" => 0,
        ];

        $this->assertEquals([
            new MysqlColumnDefinition(...\array_merge($base, ["name" => "no", "originalName" => "id", "flags" => 16899])),
            new MysqlColumnDefinition(...\array_merge($base, ["name" => "d", "originalName" => "a"])),
            new MysqlColumnDefinition(...\array_merge($base, ["name" => "b", "originalName" => "b"])),
            new MysqlColumnDefinition(...\array_merge($base, ["name" => "c", "originalName" => "c", "type" => MysqlDataType::Datetime, "length" => 19, "flags" => 128])),
        ], $stmt->getColumnDefinitions());

        $base = [
            "name" => "?",
            "catalog" => "def",
            "schema" => "",
            "table" => "",
            "originalTable" => "",
            "originalName" => "",
            "charset" => 63,
            "length" => 21,
            "flags" => 0,
            "decimals" => 0,
        ];

        $this->assertEquals([
            new MysqlColumnDefinition(...\array_merge($base, ["type" => MysqlDataType::LongLong, "flags" => 128])),
            new MysqlColumnDefinition(...\array_merge($base, ["type" => MysqlDataType::Datetime, "length" => 104, "decimals" => 6, "charset" => 45])),
            new MysqlColumnDefinition(...\array_merge($base, ["type" => MysqlDataType::VarString, "length" => 65532, "decimals" => 31, "charset" => 45])),
        ], $stmt->getParameterDefinitions());

        $stmt->bind("data", 'd');
        $result = $stmt->execute([0 => 5, 'date' => self::EPOCH]);
        $this->assertInstanceOf(MysqlResult::class, $result);
        $this->assertSame(4, $result->getColumnCount());
        $got = [];
        foreach ($result as $row) {
            $got[] = \array_values($row);
        }
        $this->assertCount(1, $got);

        $stmt = $db->prepare("SELECT * FROM main WHERE a = ? OR b = ?");
        $result = $stmt->execute([1, 8]);
        $this->assertInstanceOf(MysqlResult::class, $result);
        $this->assertSame(5, $result->getColumnCount());
        $got = [];
        foreach ($result as $row) {
            $got[] = \array_values($row);
        }
        $this->assertCount(1, $got);

        $stmt = $db->prepare("SELECT * FROM main WHERE a = :a OR b = ?");
        $result = $stmt->execute(["a" => 2, 5]);
        $this->assertInstanceOf(MysqlResult::class, $result);
        $this->assertSame(5, $result->getColumnCount());
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

        $statement->bind(1, "1");

        $statement->execute(); // Some implementations do not throw until execute() is called.
    }

    public function testBindWithInvalidParamName()
    {
        $this->expectException(\Error::class);
        $this->expectExceptionMessage('Named parameter :b is not defined for this prepared statement');

        $db = $this->getLink();

        $statement = $db->prepare("SELECT * FROM main WHERE a = :a");

        $statement->bind("b", "1");

        $statement->execute(); // Some implementations do not throw until execute() is called.
    }

    public function testStatementExecuteWithTooFewParams()
    {
        $this->expectException(\Error::class);
        $this->expectExceptionMessage('Parameter 1 missing for executing prepared statement');

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
        $this->assertSame([[2, 2, 3, self::EPOCH, 'b'], [4, 4, 5, self::EPOCH, 'd']], $got);

        $result = $db->execute("INSERT INTO main (a, b) VALUES (:a, :b)", ["a" => 10, "b" => 11, "c" => '1970-01-01 00:00:00']);
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
        $this->expectExceptionMessage('Parameter 1 missing for executing prepared statement');

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

    public function testBindJson(): void
    {
        $statement = $this->getLink()->prepare("SELECT CAST(? AS JSON)");
        $statement->bind(0, '{"key": "value"}');

        $this->expectException(SqlException::class);
        $this->expectExceptionMessage("Cannot use bind with columns of type JSON");

        $statement->execute();
    }
}
