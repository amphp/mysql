<?php

namespace Amp\Mysql\Test;

use Amp\Mysql\SocketMysqlConnector;
use Amp\Mysql\MysqlConnection;
use Amp\Mysql\MysqlLink;

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

    public function testDoubleClose()
    {
        $db = $this->getLink();

        $db->close();

        $this->assertTrue($db->isClosed());

        $db->close(); // Should not throw an exception.
    }
}
