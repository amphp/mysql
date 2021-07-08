<?php

namespace Amp\Mysql\Test;

use Amp\Mysql\CancellableConnector;
use Amp\Mysql\Connection;
use Amp\Mysql\ConnectionConfig;
use Amp\Mysql\Link;

class ConnectionTest extends LinkTest
{
    protected function getLink(string $connectionString): Link
    {
        return (new CancellableConnector)->connect(ConnectionConfig::fromString($connectionString));
    }

    public function testConnect()
    {
        $db = Connection::connect(ConnectionConfig::fromString("host=".DB_HOST." user=".DB_USER." pass=".DB_PASS." db=test"));

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
