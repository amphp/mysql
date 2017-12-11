<?php

namespace Amp\Mysql\Test;

use Amp\Delayed;
use Amp\Loop;
use Amp\Mysql\CommandResult;
use Amp\Mysql\Connection;
use Amp\Mysql\Pool;
use Amp\Mysql\ResultSet;
use Amp\Mysql\Statement;
use Amp\Mysql\Transaction;
use Amp\Promise;
use function Amp\call;
use function Amp\Mysql\connect;

abstract class AbstractPoolTest extends LinkTest {
    /**
     * @param array $connections
     *
     * @return \Amp\Mysql\Pool
     */
    abstract protected function createPool(array $connections): Pool;

    /**
     * @param string $connectionString
     *
     * @return \Amp\Promise<\Amp\Mysql\Connection>
     */
    protected function getLink(string $connectionString): Promise {
        return call(function () use ($connectionString) {
            $connection = yield connect($connectionString);
            return $this->createPool([$connection]);
        });
    }

    /**
     * @return \PHPUnit\Framework\MockObject\MockObject|\Amp\Mysql\Connection
     */
    protected function createConnection(): Connection {
        $mock = $this->createMock(Connection::class);
        $mock->method('isAlive')->willReturn(true);
        return $mock;
    }

    /**
     * @param int $count
     *
     * @return \Amp\Mysql\Connection[]|\PHPUnit\Framework\MockObject\MockObject[]
     */
    private function makeConnectionSet(int $count) {
        $connections = [];

        for ($i = 0; $i < $count; ++$i) {
            $connections[] = $this->createConnection();
        }

        return $connections;
    }

    /**
     * @return array
     */
    public function getMethodsAndResults() {
        return [
            [3, 'query', ResultSet::class, ["SELECT * FROM test"]],
            [2, 'query', CommandResult::class, ["INSERT INTO test VALUES (1, 7)"]],
            [5, 'prepare', Statement::class, ["INSERT INTO test VALUES (?, ?)"]],
            [4, 'execute', ResultSet::class, ["SELECT * FROM test WHERE a = ? AND b = ?", [1, time()]]],
        ];
    }

    /**
     * @dataProvider getMethodsAndResults
     *
     * @param int $count
     * @param string $method
     * @param string $resultClass
     * @param mixed[] $params
     */
    public function testSingleQuery(int $count, string $method, string $resultClass, array $params = []) {
        $result = $this->getMockBuilder($resultClass)
            ->disableOriginalConstructor()
            ->getMock();

        $connections = $this->makeConnectionSet($count);

        $connection = $connections[0];
        $connection->expects($this->once())
            ->method($method)
            ->with(...$params)
            ->will($this->returnValue(new Delayed(10, $result)));

        $pool = $this->createPool($connections);

        Loop::run(function () use ($method, $pool, $params, $result, $resultClass) {
            $return = yield ([$pool, $method])(...$params);
            $this->assertInstanceOf($resultClass, $return);
        });
    }

    /**
     * @dataProvider getMethodsAndResults
     *
     * @param int $count
     * @param string $method
     * @param string $resultClass
     * @param mixed[] $params
     */
    public function testConsecutiveQueries(int $count, string $method, string $resultClass, array $params = []) {
        $rounds = 3;
        $result = $this->getMockBuilder($resultClass)
            ->disableOriginalConstructor()
            ->getMock();

        $connections = $this->makeConnectionSet($count);

        foreach ($connections as $connection) {
            $connection->method($method)
                ->with(...$params)
                ->will($this->returnValue(new Delayed(10, $result)));
        }

        $pool = $this->createPool($connections);

        Loop::run(function () use ($resultClass, $count, $rounds, $pool, $method, $params) {
            $promises = [];

            for ($i = 0; $i < $count; ++$i) {
                $promises[] = ([$pool, $method])(...$params);
            }

            $results = yield $promises;

            foreach ($results as $result) {
                $this->assertInstanceOf($resultClass, $result);
            }
        });
    }

    /**
     * @return array
     */
    public function getConnectionCounts() {
        return array_map(function ($count) { return [$count]; }, range(1, 10));
    }

    /**
     * @dataProvider getConnectionCounts
     *
     * @param int $count
     */
    public function testMutlipleTransactions(int $count) {
        $connections = $this->makeConnectionSet($count);

        $connection = $connections[0];
        $result = $this->getMockBuilder(Transaction::class)
            ->disableOriginalConstructor()
            ->getMock();

        $connection->expects($this->once())
            ->method('transaction')
            ->with(Transaction::COMMITTED)
            ->will($this->returnValue(new Delayed(10, $result)));

        $pool = $this->createPool($connections);

        Loop::run(function () use ($pool, $result) {
            $return = yield $pool->transaction(Transaction::COMMITTED);
            $this->assertInstanceOf(Transaction::class, $return);
            yield $return->rollback();
        });
    }

    /**
     * @dataProvider getConnectionCounts
     *
     * @param int $count
     */
    public function testConsecutiveTransactions(int $count) {
        $rounds = 3;
        $result = $this->getMockBuilder(Transaction::class)
            ->disableOriginalConstructor()
            ->getMock();

        $connections = $this->makeConnectionSet($count);

        foreach ($connections as $connection) {
            $connection->method('transaction')
                ->with(Transaction::COMMITTED)
                ->will($this->returnCallback(function () use ($result) {
                    return new Delayed(10, $result);
                }));
        }

        $pool = $this->createPool($connections);

        Loop::run(function () use ($count, $rounds, $pool) {
            $promises = [];
            for ($i = 0; $i < $count; ++$i) {
                $promises[] = $pool->transaction(Transaction::COMMITTED);
            }

            $results = yield \array_map(function (Promise $promise): Promise {
                return call(function () use ($promise) {
                    $transaction = yield $promise;
                    $this->assertInstanceOf(Transaction::class, $transaction);
                    return yield $transaction->rollback();
                });
            }, $promises);

            foreach ($results as $result) {
                $this->assertInstanceof(CommandResult::class, $result);
            }
        });
    }

    /**
     * @dataProvider getConnectionCounts
     *
     * @param int $count
     */
    public function testExtractConnection(int $count) {
        $connections = $this->makeConnectionSet($count);
        $query = "SELECT * FROM test";

        foreach ($connections as $connection) {
            $connection->expects($this->once())
                ->method('query')
                ->with($query);
        }

        $pool = $this->createPool($connections);

        Loop::run(function () use ($pool, $query, $count) {
            $promises = [];
            for ($i = 0; $i < $count; ++$i) {
                $promises[] = $pool->extractConnection();
            }

            $results = yield $promises;

            foreach ($results as $result) {
                $this->assertInstanceof(Connection::class, $result);
                $result->query($query);
            }
        });
    }
}
