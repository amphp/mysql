<?php

namespace Amp\Mysql\Test;

use Amp\Loop;
use Amp\Mysql\ResultSet;
use Amp\Promise;
use Amp\Success;
use function Amp\Mysql\pool;

class PoolTest extends LinkTest {
    protected function getLink(string $connectionString): Promise {
        return new Success(pool($connectionString));
    }

    public function testSmallPool() {
        Loop::run(function () {
            $db = pool("host=".DB_HOST." user=".DB_USER." pass=".DB_PASS." db=test", null, 2);

            $queries = [];

            foreach (range(0, 5) as $value) {
                $queries[] = $db->query("SELECT $value");
            }

            $values = [];

            foreach ($queries as $query) {
                $result = yield $query;
                do {
                    while (yield $result->advance(ResultSet::FETCH_ARRAY)) {
                        $values[] = $result->getCurrent()[0];
                    }
                } while (yield $result->nextResultSet());
            }

            $this->assertEquals(\range(0, 5), $values);
        });
    }

    /**
     * @expectedException \Amp\Mysql\InitializationException
     * @expectedExceptionMessage Access denied for user
     */
    public function testWrongPassword() {
        Loop::run(function () {
            $db = pool("host=".DB_HOST.";user=".DB_USER.";pass=the_wrong_password;db=test");

            /* Try a query */
            yield $db->query("CREATE TABLE tmp SELECT 1 AS a, 2 AS b");
        });
    }
}
