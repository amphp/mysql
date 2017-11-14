<?php

/*
 * Generic example for establishing a connection
 */

require 'support/bootstrap.php';

Amp\Loop::run(function () {
    /* If you want ssl, pass as second argument an array with ssl options (an empty options array is valid too); if null is passed, ssl is not enabled either */
    $db = yield \Amp\Mysql\connect("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=".DB_NAME);

    /* use an alternative charset... Default is utf8mb4_general_ci */
    yield $db->setCharset("latin1_general_ci");

    echo "Character set changed\n";

    /* optional, as connection will automatically close when destructed. */
    $db->close();
});
