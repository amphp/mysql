<?php

namespace Amp\Mysql\Test;

use Amp\Mysql\CancellableConnector;
use Amp\Mysql\Connection;
use Amp\Mysql\ConnectionConfig;
use Amp\Promise;

class ConnectionTest extends LinkTest
{
    protected function getLink(string $connectionString): Promise
    {
        return (new CancellableConnector())->connect(ConnectionConfig::fromString($connectionString));
    }

    public function testConnect()
    {
        /** @var Connection $db */
        $db = yield Connection::connect(ConnectionConfig::fromString("host=".DB_HOST." user=".DB_USER." pass=".DB_PASS." db=test"));

        $this->assertInstanceOf(Connection::class, $db);

        /* use an alternative charset... Default is utf8mb4_general_ci */
        yield $db->setCharset("latin1_general_ci");

        $db->close();
    }

    public function testDoubleClose()
    {
        /** @var Connection $db */
        $db = yield $this->getLink("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=test");

        $db->close();

        $this->assertFalse($db->isAlive());

        $db->close(); // Should not throw an exception.
    }
}
