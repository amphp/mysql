<?php

namespace Amp\Mysql\Test;

use Amp\Loop;
use Amp\Mysql\ConnectionConfig;
use Amp\Mysql\TimeoutConnector;
use Amp\Promise;
use function Amp\Mysql\connect;

class ConnectionTest extends LinkTest
{
    protected function getLink(string $connectionString): Promise
    {
        return (new TimeoutConnector)->connect(ConnectionConfig::fromString($connectionString));
    }

    public function testConnect()
    {
        $complete = false;
        Loop::run(function () use (&$complete) {
            /** @var \Amp\Mysql\Connection $db */
            $db = yield connect(ConnectionConfig::fromString("host=".DB_HOST." user=".DB_USER." pass=".DB_PASS." db=test"));

            /* use an alternative charset... Default is utf8mb4_general_ci */
            yield $db->setCharset("latin1_general_ci");

            $db->close();
            $complete = true;
        });
        $this->assertTrue($complete, "Database commands did not complete.");
    }

    /**
     * @expectedException \Error
     * @expectedExceptionMessage Host must be provided in connection string
     */
    public function testInvalidConnectionString()
    {
        $promise = connect(ConnectionConfig::fromString("username=".DB_USER));
    }

    public function testDoubleClose()
    {
        Loop::run(function () {
            /** @var \Amp\Mysql\Connection $db */
            $db = yield $this->getLink("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=test");

            $db->close();

            $this->assertFalse($db->isAlive());

            $db->close(); // Should not throw an exception.
        });
    }
}
