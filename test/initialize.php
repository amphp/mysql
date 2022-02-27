<?php

namespace Amp\Mysql\Test;

function initialize(\mysqli $db): void
{
    $db->query("CREATE DATABASE test");
    $db->query("CREATE TABLE test.main (id INT NOT NULL AUTO_INCREMENT, a INT, b INT, PRIMARY KEY (id))");
    $db->query("INSERT INTO test.main (a, b) VALUES (1, 2), (2, 3), (3, 4), (4, 5), (5, 6)");
    $db->close();
}
