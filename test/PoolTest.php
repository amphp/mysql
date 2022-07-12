<?php

namespace Amp\Mysql\Test;

use Amp\Future;
use Amp\Mysql\Internal\ConnectionProcessor;
use Amp\Mysql\Internal\MysqlCommandResult;
use Amp\Mysql\MysqlConfig;
use Amp\Mysql\MysqlConnection;
use Amp\Mysql\MysqlConnectionPool;
use Amp\Mysql\MysqlConnector;
use Amp\Mysql\MysqlLink;
use Amp\Mysql\MysqlResult;
use Amp\Mysql\MysqlStatement;
use Amp\Mysql\SocketMysqlConnection;
use Amp\Sql\Transaction as SqlTransaction;
use PHPUnit\Framework\MockObject\MockObject;
use function Amp\async;
use function Amp\delay;

interface StatementOperation extends MysqlStatement
{
}

class PoolTest extends LinkTest
{
    protected function getLink(bool $useCompression = false): MysqlLink
    {
        return new MysqlConnectionPool($this->getConfig($useCompression));
    }

    protected function createPool(array $connections): MysqlConnectionPool
    {
        $connector = $this->createMock(MysqlConnector::class);
        $connector->method('connect')
            ->will($this->returnCallback(function () use ($connections): MysqlConnection {
                static $count = 0;
                return $connections[$count++ % \count($connections)];
            }));

        $config = MysqlConfig::fromString('host=host;user=user;password=password');

        return new MysqlConnectionPool($config, \count($connections), MysqlConnectionPool::DEFAULT_IDLE_TIMEOUT, $connector);
    }

    /**
     * @return array<int, ConnectionProcessor&MockObject>
     */
    private function makeProcessorSet(int $count): array
    {
        $processors = [];

        for ($i = 0; $i < $count; ++$i) {
            $processor = $this->createMock(ConnectionProcessor::class);
            $processor->method('isClosed')->willReturn(false);
            $processor->method('sendClose')->willReturn(Future::complete(null));
            $processors[] = $processor;
        }

        return $processors;
    }

    private function makeConnectionSet(array $processors): array
    {
        return \array_map((function (ConnectionProcessor $processor): SocketMysqlConnection {
            return new self($processor);
        })->bindTo(null, SocketMysqlConnection::class), $processors);
    }

    public function getConnectionCounts(): array
    {
        return \array_map(function (int $count): array { return [$count]; }, \range(2, 10, 2));
    }

    /**
     * @dataProvider getConnectionCounts
     */
    public function testSingleQuery(int $count)
    {
        $result = $this->createMock(MysqlResult::class);

        $processors = $this->makeProcessorSet($count);

        $connection = $processors[0];
        $connection->expects($this->once())
            ->method('query')
            ->with('SQL Query')
            ->willReturn(async(function () use ($result): MysqlResult {
                delay(0.01);
                return $result;
            }));

        $pool = $this->createPool($this->makeConnectionSet($processors));

        $return = $pool->query('SQL Query');
        $this->assertInstanceOf(MysqlResult::class, $return);

        $pool->close();
    }

    /**
     * @dataProvider getConnectionCounts
     */
    public function testConsecutiveQueries(int $count)
    {
        $rounds = 3;
        $result = $this->createMock(MysqlResult::class);

        $processors = $this->makeProcessorSet($count);

        foreach ($processors as $connection) {
            $connection->method('query')
                ->with('SQL Query')
                ->willReturn(async(function () use ($result): MysqlResult {
                    delay(0.01);
                    return $result;
                }));
        }

        $pool = $this->createPool($this->makeConnectionSet($processors));

        try {
            $futures = [];

            for ($i = 0; $i < $count; ++$i) {
                $futures[] = async(fn () => $pool->query('SQL Query'));
            }

            $results = Future\await($futures);

            foreach ($results as $result) {
                $this->assertInstanceOf(MysqlResult::class, $result);
            }
        } finally {
            $pool->close();
        }
    }

    /**
     * @dataProvider getConnectionCounts
     */
    public function testMultipleTransactions(int $count)
    {
        $processors = $this->makeProcessorSet($count);

        $connection = $processors[0];
        $result = new MysqlCommandResult(0, 0);

        $connection->expects($this->exactly(3))
            ->method('query')
            ->willReturn(async(function () use ($result): MysqlResult {
                delay(0.01);
                return $result;
            }));

        $pool = $this->createPool($this->makeConnectionSet($processors));

        try {
            $return = $pool->beginTransaction();
            $this->assertInstanceOf(SqlTransaction::class, $return);
            $return->rollback();
        } finally {
            $pool->close();
        }
    }

    /**
     * @dataProvider getConnectionCounts
     */
    public function testConsecutiveTransactions(int $count)
    {
        $rounds = 3;
        $result = new MysqlCommandResult(0, 0);

        $processors = $this->makeProcessorSet($count);

        foreach ($processors as $connection) {
            $connection->method('query')
                ->willReturnCallback(fn () => async(function () use ($result): MysqlResult {
                    delay(0.01);
                    return $result;
                }));
        }

        $pool = $this->createPool($this->makeConnectionSet($processors));

        $futures = [];
        for ($i = 0; $i < $count; ++$i) {
            $futures[] = async(fn () => $pool->beginTransaction());
        }

        try {
            \array_map(function (Future $future) {
                $transaction = $future->await();
                $this->assertInstanceOf(SqlTransaction::class, $transaction);
                $transaction->rollback();
            }, $futures);
        } finally {
            $pool->close();
        }
    }

    /**
     * @dataProvider getConnectionCounts
     */
    public function testExtractConnection(int $count)
    {
        $processors = $this->makeProcessorSet($count);
        $query = "SELECT * FROM test";

        foreach ($processors as $connection) {
            $connection->expects($this->once())
                ->method('query')
                ->with($query)
                ->willReturn(Future::complete($this->createMock(MysqlResult::class)));
        }

        $pool = $this->createPool($this->makeConnectionSet($processors));

        try {
            $futures = [];
            for ($i = 0; $i < $count; ++$i) {
                $futures[] = async(fn () => $pool->extractConnection());
            }
            $results = Future\await($futures);
            foreach ($results as $result) {
                $this->assertInstanceof(MysqlConnection::class, $result);
                $result->query($query);
            }
        } finally {
            $pool->close();
        }
    }

    /**
     * @dataProvider getConnectionCounts
     */
    public function testConnectionClosedInPool(int $count)
    {
        $processors = $this->makeProcessorSet($count);
        $query = "SELECT * FROM test";
        $result = $this->createMock(MysqlResult::class);

        foreach ($processors as $processor) {
            $processor->expects($this->atLeastOnce())
                ->method('query')
                ->with($query)
                ->willReturn(async(function () use ($result): MysqlResult {
                    delay(0.01);
                    return $result;
                }));
        }

        $processor = $this->createMock(ConnectionProcessor::class);
        $processor->method('isClosed')
            ->willReturnOnConsecutiveCalls(false, true);
        $processor->expects($this->once())
            ->method('query')
            ->with($query)
            ->willReturn(async(function () use ($result): MysqlResult {
                delay(0.01);
                return $result;
            }));

        \array_unshift($processors, $processor);

        $pool = $this->createPool($this->makeConnectionSet($processors));

        try {
            $this->assertSame($count + 1, $pool->getConnectionLimit());
            $futures = [];
            for ($i = 0; $i < $count + 1; ++$i) {
                $futures[] = async(fn () => $pool->query($query));
            }
            Future\await($futures);
            $futures = [];
            for ($i = 0; $i < $count; ++$i) {
                $futures[] = async(fn () => $pool->query($query));
            }
            Future\await($futures);
        } finally {
            $pool->close();
        }
    }
}
