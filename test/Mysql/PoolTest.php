<?php

use PHPUnit\Framework\TestCase;
use function Amp\Mysql\pool;

class PoolTest extends TestCase {
    /**
     * @expectedException \Amp\Mysql\InitializationException
     * @expectedExceptionMessage Access denied for user
     */
    function testWrongPassword() {
        \Amp\Loop::run(function() {
            $db = pool("host=".DB_HOST.";user=".DB_USER.";pass=the_wrong_password;db=connectiontest");

            /* Try a query */
            yield $db->query("CREATE TABLE tmp SELECT 1 AS a, 2 AS b");
        });
    }
}