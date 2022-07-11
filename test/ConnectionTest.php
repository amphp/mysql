<?php

namespace Amp\Mysql\Test;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\Mysql\MysqlConnection;
use Amp\Mysql\MysqlLink;
use Amp\Mysql\SocketMysqlConnector;
use function Amp\Mysql\connect;

class ConnectionTest extends LinkTest
{
    protected function getLink(bool $useCompression = false): MysqlLink
    {
        return (new SocketMysqlConnector)->connect($this->getConfig($useCompression));
    }

    public function testConnect()
    {
        $connector = new SocketMysqlConnector();

        $db = $connector->connect($this->getConfig());

        $this->assertInstanceOf(MysqlConnection::class, $db);

        /* use an alternative charset... Default is utf8mb4_general_ci */
        $db->setCharset("latin1", "latin1_general_ci");

        $db->close();
    }

    /**
     * @depends testConnect
     */
    public function testConnectCancellationBeforeConnect()
    {
        $this->expectException(CancelledException::class);

        $connector = new SocketMysqlConnector();

        $source = new DeferredCancellation;
        $cancellation = $source->getCancellation();
        $source->cancel();
        $connector->connect($this->getConfig(), $cancellation);
    }

    /**
     * @depends testConnectCancellationBeforeConnect
     */
    public function testConnectCancellationAfterConnect()
    {
        $connector = new SocketMysqlConnector();

        $source = new DeferredCancellation;
        $cancellation = $source->getCancellation();
        $connection = $connector->connect($this->getConfig(), $cancellation);
        $this->assertInstanceOf(MysqlConnection::class, $connection);
        $source->cancel();
    }

    /**
     * @depends testConnect
     */
    public function testConnectFunction()
    {
        $connection = connect($this->getConfig());
        $this->assertInstanceOf(MysqlConnection::class, $connection);
    }

    /**
     * @depends testConnectFunction
     */
    public function testCancelConnect()
    {
        $this->expectException(CancelledException::class);

        $source = new DeferredCancellation;
        $cancellation = $source->getCancellation();
        $source->cancel();
        connect($this->getConfig(), $cancellation);
    }

    public function testDoubleClose()
    {
        $db = $this->getLink();

        $db->close();

        $this->assertTrue($db->isClosed());

        $db->close(); // Should not throw an exception.
    }
}
