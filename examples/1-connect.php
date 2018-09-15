<?php

/*
 * Generic example for establishing a connection
 */

require 'support/bootstrap.php';

use Amp\Mysql;

Amp\Loop::run(function () {
    /* If you want ssl, pass as second argument an array with ssl options (an empty options array is valid too); if null is passed, ssl is not enabled either */
    $config = Mysql\ConnectionConfig::fromString("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=".DB_NAME);
    /* use an alternative charset... Default is utf8mb4_general_ci */
    $config = $config->withCharset("latin1_general_ci");
    /** @var Mysql\Connection $db */
    $db = yield Mysql\connect($config);

    echo "Character set changed\n";

    /* optional, as connection will automatically close when destructed. */
    $db->close();
});
