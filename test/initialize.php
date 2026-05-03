<?php declare(strict_types=1);

namespace Amp\Mysql\Test;

const EXPECTED_COLUMN_COUNT = 6;

function initialize(\mysqli $db): void
{
    $db->query("CREATE DATABASE test");

    $db->query(<<<SQL
        CREATE TABLE test.main (
            id INT NOT NULL AUTO_INCREMENT,
            a INT NULL,
            b INT NULL,
            c DATETIME NULL,
            d VARCHAR(255) NULL,
            e BLOB NULL,
            PRIMARY KEY (id)
        );
    SQL);

    $epoch = MysqlLinkTest::EPOCH;
    $db->query(<<<SQL
        INSERT INTO
            test.main (a, b, c, d)
        VALUES
            (1, 2, '$epoch', 'a'),
            (2, 3, '$epoch', 'b'),
            (3, 4, '$epoch', 'c'),
            (4, 5, '$epoch', 'd'), 
            (5, 6, '$epoch', 'e')
    SQL);

    $db->close();
}
