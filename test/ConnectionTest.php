<?php

namespace Amp\Mysql\Test;

use Amp\Mysql\Connection;
use Amp\Mysql\DefaultMysqlConnector;
use Amp\Mysql\Link;
use Amp\Mysql\MysqlConfig;

class ConnectionTest extends LinkTest
{
    protected function getLink(string $connectionString): Link
    {
        return (new DefaultMysqlConnector)->connect(MysqlConfig::fromString($connectionString));
    }

    public function testConnect()
    {
        $connector = new DefaultMysqlConnector();

        $db = $connector->connect(MysqlConfig::fromString("host=".DB_HOST." user=".DB_USER." pass=".DB_PASS." db=test"));

        $this->assertInstanceOf(Connection::class, $db);

        /* use an alternative charset... Default is utf8mb4_general_ci */
        $db->setCharset("latin1_general_ci");

        $db->close();
    }

    public function testDoubleClose()
    {
        $db = $this->getLink("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=test");

        $db->close();

        $this->assertFalse($db->isAlive());

        $db->close(); // Should not throw an exception.
    }
}
