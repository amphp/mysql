<?php

require __DIR__ . '/../../vendor/autoload.php';

/*
 * This file is not in VCS - create it if you want to set
 * the DB credentials without having them commited to VCS.
 */
@include_once __DIR__ . "/../../mysql-config.php";

if (!defined('DB_HOST') || !defined('DB_USER') || !defined('DB_PASS') || !defined('DB_NAME')) {
    throw new \Exception("Must create mysql-config.php in project root directory defining connection constants");
}
