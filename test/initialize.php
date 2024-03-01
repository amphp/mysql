<?php declare(strict_types=1);

namespace Amp\Mysql\Test;

function initialize(\mysqli $db): void
{
    $db->query("CREATE DATABASE test");

    $db->query("CREATE TABLE test.main (id INT NOT NULL AUTO_INCREMENT, a INT, b INT, c DATETIME, d VARCHAR(255), PRIMARY KEY (id))");

    $epoch = MysqlLinkTest::EPOCH;
    $db->query("INSERT INTO test.main (a, b, c, d) VALUES (1, 2, '$epoch', 'a'), (2, 3, '$epoch', 'b'), (3, 4, '$epoch', 'c'), (4, 5, '$epoch', 'd'), (5, 6, '$epoch', 'e')");

    $db->close();
}
