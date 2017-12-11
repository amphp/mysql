<?php

namespace Amp\Mysql\Test;

use Amp\Mysql\Connection;
use Amp\Mysql\Internal\ConnectionConfig;
use Amp\Promise;
use function Amp\Mysql\connect;

class ConnectionTest extends LinkTest {
    protected function getLink(string $connectionString): Promise {
        return Connection::connect(ConnectionConfig::parseConnectionString($connectionString));
    }

    public function testConnect() {
        $complete = false;
        \Amp\Loop::run(function () use (&$complete) {
            /** @var \Amp\Mysql\Connection $db */
            $db = yield connect("host=".DB_HOST." user=".DB_USER." pass=".DB_PASS." db=test");

            /* use an alternative charset... Default is utf8mb4_general_ci */
            yield $db->setCharset("latin1_general_ci");

            $db->close();
            $complete = true;
        });
        $this->assertTrue($complete, "Database commands did not complete.");
    }

    /**
     * @expectedException \Error
     * @expectedExceptionMessage Required parameters host, user and pass need to be passed in connection string
     */
    public function testInvalidConnectionString() {
        $promise = connect("username=".DB_USER);
    }
}
