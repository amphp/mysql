<?php

namespace Amp\Mysql\Test;

use Amp\Iterator;
use Amp\Mysql\CommandResult;
use Amp\Mysql\DataTypes;
use Amp\Mysql\ResultSet;
use Amp\Mysql\Statement;
use Amp\PHPUnit\AsyncTestCase;
use Amp\Promise;
use Amp\Sql\Link;
use Amp\Sql\QueryError;
use Amp\Sql\Transaction;

abstract class LinkTest extends AsyncTestCase
{
    /**
     * Returns the Link class to be tested.
     *
     * @return Promise<Link>
     */
    abstract protected function getLink(string $connectionString): Promise;

    public function testQuery()
    {
        /** @var Link $db */
        $db = yield $this->getLink("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=test");

        /** @var ResultSet $resultset */
        $resultset = yield $db->execute("SELECT ? AS a", [M_PI]);
        $this->assertInstanceOf(ResultSet::class, $resultset);

        for ($i = 0; yield $resultset->advance(); ++$i) {
            $this->assertSame(["a" => M_PI], $resultset->getCurrent());
        }

        $this->assertSame(1, $i);
    }

    public function testQueryFetchRow()
    {
        /** @var Link $db */
        $db = yield $this->getLink("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=test");

        /** @var ResultSet $resultset */
        $resultset = yield $db->query('SELECT a FROM main WHERE a < 4');
        $this->assertInstanceOf(ResultSet::class, $resultset);

        $got = [];
        while (yield $resultset->advance()) {
            $got[] = \array_values($resultset->getCurrent());
        }

        $this->assertSame($got, [[1], [2], [3]]);
    }

    public function testQueryWithInvalidQuery()
    {
        $this->expectException(QueryError::class);
        $this->expectExceptionMessage('You have an error in your SQL syntax');

        /** @var Link $db */
        $db = yield $this->getLink("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=test");

        yield $db->query("SELECT & FROM main WHERE a = 1");
    }

    public function testMultiStmt()
    {
        /** @var Link $db */
        $db = yield $this->getLink("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=test;useCompression=true");

        /** @var ResultSet $resultset */
        $resultset = yield $db->query("SELECT a FROM main; SELECT b FROM main WHERE a = 5; SELECT b AS d, a + 1 AS c FROM main WHERE b > 4");
        $this->assertInstanceOf(ResultSet::class, $resultset);

        $got = [];
        while (yield $resultset->advance()) {
            $got[] = \array_values($resultset->getCurrent());
        }
        $this->assertSame([[1], [2], [3], [4], [5]], $got);
        $this->assertTrue(yield $resultset->nextResultSet());

        $got = [];
        while (yield $resultset->advance()) {
            $got[] = \array_values($resultset->getCurrent());
        }
        $this->assertSame([[6]], $got);
        $this->assertTrue(yield $resultset->nextResultSet());

        $fields = yield $resultset->getFields();

        $got = [];
        while (yield $resultset->advance()) {
            $got[] = $resultset->getCurrent();
        }
        $this->assertSame([["d" => 5, "c" => 5], ["d" => 6, "c" => 6]], $got);

        $this->assertCount(2, $fields);
        $this->assertSame($fields[0]["original_name"], "b");
        $this->assertSame($fields[0]["name"], "d");
        $this->assertSame($fields[0]["type"], DataTypes::MYSQL_TYPE_LONG);
        $this->assertSame($fields[1]["name"], "c");
        $this->assertSame($fields[1]["type"], DataTypes::MYSQL_TYPE_LONGLONG);

        $this->assertFalse(yield $resultset->nextResultSet());
    }

    public function testPrepared()
    {
        /** @var Link $db */
        $db = yield $this->getLink("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=test;useCompression=true");

        /**
         * @var Statement $stmt
         * @var ResultSet $result
         */
        $stmt = yield $db->prepare("SELECT * FROM main WHERE a = ? OR b = :num");
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
        $this->assertEquals(yield $stmt->getFields(), [$base + ["name" => "a", "original_name" => "a"], $base + ["name" => "b", "original_name" => "b"]]);
        $stmt->bind("num", 5);
        $result = yield $stmt->execute([2]);
        $this->assertInstanceOf(ResultSet::class, $result);
        $got = [];
        while (yield $result->advance()) {
            $got[] = \array_values($result->getCurrent());
        }
        $this->assertCount(2, $got);

        $stmt = yield $db->prepare("SELECT * FROM main WHERE a = ? OR b = ?");
        $result = yield $stmt->execute([1, 8]);
        $this->assertInstanceOf(ResultSet::class, $result);
        $got = [];
        while (yield $result->advance()) {
            $got[] = \array_values($result->getCurrent());
        }
        $this->assertCount(1, $got);

        $stmt = yield $db->prepare("SELECT * FROM main WHERE a = :a OR b = ?");
        $result = yield $stmt->execute(["a" => 2, 5]);
        $this->assertInstanceOf(ResultSet::class, $result);
        $got = [];
        while (yield $result->advance()) {
            $got[] = \array_values($result->getCurrent());
        }
        $this->assertCount(2, $got);

        $stmt = yield $db->prepare("INSERT INTO main VALUES (:a, :b)");
        $result = yield $stmt->execute(["a" => 10, "b" => 11]);
        $this->assertInstanceOf(CommandResult::class, $result);

        $stmt = yield $db->prepare("DELETE FROM main WHERE a = :a");
        $result = yield $stmt->execute(["a" => 10]);
        $this->assertInstanceOf(CommandResult::class, $result);
    }

    public function testPrepareWithInvalidQuery()
    {
        $this->expectException(QueryError::class);
        $this->expectExceptionMessage('You have an error in your SQL syntax');

        /** @var \Amp\Sql\Link $db */
        $db = yield $this->getLink("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=test");

        yield $db->prepare("SELECT & FROM main WHERE a = ?");
    }

    public function testBindWithInvalidParamId()
    {
        $this->expectException(\Error::class);
        $this->expectExceptionMessage('Parameter id 1 is not defined for this prepared statement');

        /** @var Link $db */
        $db = yield $this->getLink("host=" . DB_HOST . ";user=" . DB_USER . ";pass=" . DB_PASS . ";db=test");

        /** @var Statement $statement */
        $statement = yield $db->prepare("SELECT * FROM main WHERE a = ?");

        $statement->bind(1, 1);

        yield $statement->execute(); // Some implementations do not throw until execute() is called.
    }

    public function testBindWithInvalidParamName()
    {
        $this->expectException(\Error::class);
        $this->expectExceptionMessage('Parameter :b is not defined for this prepared statement');

        /** @var Link $db */
        $db = yield $this->getLink("host=" . DB_HOST . ";user=" . DB_USER . ";pass=" . DB_PASS . ";db=test");

        /** @var Statement $statement */
        $statement = yield $db->prepare("SELECT * FROM main WHERE a = :a");

        $statement->bind("b", 1);

        yield $statement->execute(); // Some implementations do not throw until execute() is called.
    }

    public function testBindWithInvalidParamType()
    {
        $this->expectException(\Error::class);
        $this->expectExceptionMessage('Invalid parameter ID type');

        /** @var Link $db */
        $db = yield $this->getLink("host=" . DB_HOST . ";user=" . DB_USER . ";pass=" . DB_PASS . ";db=test");

        /** @var Statement $statement */
        $statement = yield $db->prepare("SELECT * FROM main WHERE a = :a");

        $statement->bind(3.14, 1);

        yield $statement->execute(); // Some implementations do not throw until execute() is called.
    }

    public function testStatementExecuteWithTooFewParams()
    {
        $this->expectException(\Error::class);
        $this->expectExceptionMessage('Parameter 1 for prepared statement missing');

        /** @var Link $db */
        $db = yield $this->getLink("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=test");

        /** @var Statement $stmt */
        $stmt = yield $db->prepare("SELECT * FROM main WHERE a = ? AND b = ?");
        yield $stmt->execute([1]);
    }

    public function testExecute()
    {
        /** @var Link $db */
        $db = yield $this->getLink("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=test");

        /** @var ResultSet $result */
        $result = yield $db->execute("SELECT * FROM main WHERE a = ? OR b = ?", [2, 5]);
        $this->assertInstanceOf(ResultSet::class, $result);
        $got = [];
        while (yield $result->advance()) {
            $got[] = \array_values($result->getCurrent());
        }
        $this->assertCount(2, $got);
        $this->assertSame([[2, 3], [4, 5]], $got);

        $result = yield $db->execute("INSERT INTO main VALUES (:a, :b)", ["a" => 10, "b" => 11]);
        $this->assertInstanceOf(CommandResult::class, $result);

        $result = yield $db->execute("DELETE FROM main WHERE a = :a", ["a" => 10]);
        $this->assertInstanceOf(CommandResult::class, $result);
    }

    public function testExecuteWithInvalidQuery()
    {
        $this->expectException(QueryError::class);
        $this->expectExceptionMessage('You have an error in your SQL syntax');

        /** @var Link $db */
        $db = yield $this->getLink("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=test");

        yield $db->execute("SELECT & FROM main WHERE a = ?", [1]);
    }

    public function testExecuteWithTooFewParams()
    {
        $this->expectException(\Error::class);
        $this->expectExceptionMessage('Parameter 1 for prepared statement missing');

        /** @var Link $db */
        $db = yield $this->getLink("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=test");

        yield $db->execute("SELECT * FROM main WHERE a = ? AND b = ?", [1]);
    }

    public function testPreparedWithNegativeValue()
    {
        /** @var Link $db */
        $db = yield $this->getLink("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=test");

        yield $db->query("DROP TABLE IF EXISTS tmp");

        /** @var \Amp\Sql\Statement $stmt */
        $stmt = yield $db->prepare("CREATE TABLE tmp SELECT ? AS a");
        yield $stmt->execute([-1]);

        /** @var \Amp\Mysql\ResultSet $result */
        $stmt = yield $db->prepare("SELECT a FROM tmp");
        $result = yield $stmt->execute();
        yield $result->advance();

        $this->assertEquals(\array_values($result->getCurrent()), [-1]);
    }

    public function testTransaction()
    {
        /** @var Link $db */
        $db = yield $this->getLink("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=test");

        /** @var Transaction $transaction */
        $transaction = yield $db->beginTransaction();

        /** @var Statement $statement */
        $statement = yield $transaction->prepare("INSERT INTO main VALUES (?, ?)");
        $result = yield $statement->execute([6, 7]);
        $this->assertInstanceOf(CommandResult::class, $result);

        /** @var ResultSet $result */
        $result = yield $transaction->execute("SELECT * FROM main WHERE a = ?", [6]);

        $got = [];
        while (yield $result->advance()) {
            $got[] = \array_values($result->getCurrent());
        }
        $this->assertCount(1, $got);
        yield $result->nextResultSet();

        yield $transaction->rollback();

        $result = yield $db->execute("SELECT * FROM main WHERE a = ?", [6]);

        $got = [];
        while (yield $result->advance()) {
            $got[] = \array_values($result->getCurrent());
        }
        $this->assertCount(0, $got);
    }

    /**
     * @depends testTransaction
     */
    public function testInsertSelect()
    {
        /** @var Link $db */
        $db = yield $this->getLink("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=test");

        $a = 1;

        /** @var Transaction $transaction */
        $transaction = yield $db->beginTransaction();

        try {
            /** @var Statement $statement */
            $statement = yield $transaction->prepare("SELECT a, b FROM main WHERE a >= ?");

            $count = \count(yield Iterator\toArray(yield $statement->execute([$a])));

            /** @var Statement $statement */
            $statement = yield $transaction->prepare("INSERT INTO main (a, b) SELECT a, b FROM main WHERE a >= ?");

            /** @var CommandResult $result */
            $result = yield $statement->execute([$a]);

            $this->assertSame($count, $result->getAffectedRowCount());
        } finally {
            yield $transaction->rollback();
        }
    }
}
