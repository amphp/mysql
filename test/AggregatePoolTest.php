<?php

namespace Amp\Mysql\Test;

use Amp\Mysql\AggregatePool;
use Amp\Mysql\Connection;
use Amp\Mysql\Pool;

class AggregatePoolTest extends AbstractPoolTest {
    /**
     * @param array $connections
     *
     * @return \PHPUnit_Framework_MockObject_MockObject|\Amp\Mysql\Pool
     */
    protected function createPool(array $connections): Pool {
        $mock = $this->getMockBuilder(AggregatePool::class)
            ->setConstructorArgs(['', 0, count($connections)])
            ->setMethods(['createConnection'])
            ->getMock();

        $mock->method('createConnection')
            ->will($this->returnCallback(function () {
                $this->fail('The createConnection() method should not be called.');
            }));

        foreach ($connections as $connection) {
            $mock->addConnection($connection);
        }

        return $mock;
    }

    public function testGetMaxConnections() {
        $pool = $this->createPool([$this->createConnection()]);
        $this->assertSame(1, $pool->getMaxConnections());
        $pool->addConnection($this->createConnection());
        $this->assertSame(2, $pool->getMaxConnections());
    }

    public function testGetConnectionCount() {
        $pool = $this->createPool([$this->createConnection(), $this->createConnection()]);
        $this->assertSame(2, $pool->getConnectionCount());
    }

    public function testGetIdleConnectionCount() {
        $pool = $this->createPool([$this->createConnection(), $this->createConnection()]);
        $this->assertSame(2, $pool->getIdleConnectionCount());
        $promise = $pool->query("SELECT 1");
        $this->assertSame(1, $pool->getIdleConnectionCount());
    }

    /**
     * @expectedException \Error
     * @expectedExceptionMessage Connection is already a part of this pool
     */
    public function testDoubleAddConnection() {
        $pool = $this->createPool([]);
        $connection = $this->createConnection();
        $pool->addConnection($connection);
        $pool->addConnection($connection);
    }

    /**
     * @expectedException \Error
     * @expectedExceptionMessage The connection is dead
     */
    public function testAddDeadConnection() {
        $pool = $this->createPool([]);
        $connection = $this->createMock(Connection::class);
        $connection->method('isAlive')->willReturn(false);
        $pool->addConnection($connection);
    }

    /**
     * @expectedException \Error
     * @expectedExceptionMessage The pool has been closed
     */
    public function testAddConnectionAfterClose() {
        $pool = $this->createPool([]);
        $pool->close();
        $connection = $this->createMock(Connection::class);
        $connection->method('isAlive')->willReturn(false);
        $pool->addConnection($connection);
    }
}
