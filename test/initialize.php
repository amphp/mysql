<?php

namespace Amp\Mysql\Test;

function initialize(\mysqli $db): void
{
    $db->query("CREATE DATABASE test");

    $db->query("CREATE TABLE test.main (id INT NOT NULL AUTO_INCREMENT, a INT, b INT, c DATETIME, PRIMARY KEY (id))");

    $epoch = LinkTest::EPOCH;
    $db->query("INSERT INTO test.main (a, b, c) VALUES (1, 2, '$epoch'), (2, 3, '$epoch'), (3, 4, '$epoch'), (4, 5, '$epoch'), (5, 6, '$epoch')");

    $db->query("CREATE TABLE test.json (a JSON)");
    $db->query("INSERT INTO test.json VALUES ('{\"key\": \"value\"}')");

    $db->close();
}
