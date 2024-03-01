<?php declare(strict_types=1);

namespace Amp\Mysql\Test;

use Amp\Mysql\MysqlConnection;
use Amp\Mysql\MysqlLink;
use Amp\Mysql\MysqlTransaction;
use Amp\Mysql\SocketMysqlConnector;
use function Amp\async;

class MysqlNestedTransactionTest extends MysqlLinkTest
{
    private ?MysqlLink $link;
    private ?MysqlTransaction $transaction = null;
    private ?MysqlTransaction $nested = null;

    public function getLink(bool $useCompression = false): MysqlLink
    {
        $this->link = $this->connect($useCompression);
        $this->transaction = $this->link->beginTransaction();
        $this->nested = $this->transaction->beginTransaction();

        return $this->nested;
    }

    public function tearDown(): void
    {
        if ($this->nested?->isActive()) {
            $this->nested->rollback();
        }

        if ($this->transaction?->isActive()) {
            $this->transaction->rollback();
        }

        $this->link?->close();

        parent::tearDown();
    }

    protected function connect(bool $useCompression = false): MysqlConnection
    {
        return (new SocketMysqlConnector)->connect($this->getConfig($useCompression));
    }

    public function testStatementExecuteWaitsForNestedTransaction(): void
    {
        $sql = "SELECT * FROM main WHERE a = :a";
        $params = ["a" => 1];

        $this->link = (new SocketMysqlConnector)->connect($this->getConfig());
        $this->transaction = $this->link->beginTransaction();

        $stmt = $this->transaction->prepare($sql);
        $result = \iterator_to_array($stmt->execute($params));

        $this->nested = $this->transaction->beginTransaction();

        $future = async(fn () => \iterator_to_array($stmt->execute($params)));

        $nestedStmt = $this->nested->prepare($sql);
        self::assertSame($result, \iterator_to_array($nestedStmt->execute($params)));

        self::assertFalse($future->isComplete());

        $this->nested->commit();

        self::assertSame($result, $future->await());
    }

    public function testRollbackInnerCommitOuter(): void
    {
        $sql = "INSERT INTO main (a, b) VALUES (:a, :b)";

        $this->link = $this->connect();

        $this->transaction = $this->link->beginTransaction();

        $nested1 = $this->transaction->beginTransaction();
        $nested1->onCommit($this->createCallback(0));
        $nested1->onRollback($this->createCallback(1));

        $nested1->execute($sql, ["a" => 10, "b" => 20]);

        $nested2 = $nested1->beginTransaction();
        $nested2->onCommit($this->createCallback(0));
        $nested2->onRollback($this->createCallback(1));

        $nested2->execute($sql, ["a" => 20, "b" => 30]);

        $nested2->rollback();

        $nested1->commit();

        $sql = "SELECT * FROM main WHERE a = :a AND b = :b";

        self::assertCount(1, \iterator_to_array($this->transaction->execute($sql, ["a" => 10, "b" => 20])));
        self::assertCount(0, \iterator_to_array($this->transaction->execute($sql, ["a" => 20, "b" => 30])));

        $this->transaction->rollback(); // Trigger onRollback callback within test.
    }
}
