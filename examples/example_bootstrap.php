<?php

require '../vendor/autoload.php';

// This file is not in VCS - create it if you want to set 
// the DB credentials without having them commited to VCS.
include_once "./mysql_config.php";

if (!defined('DB_HOST')) {
	define('DB_HOST', '');
}
if (!defined('DB_USER')){
	define('DB_USER', '');
}
if (!defined('DB_PASS')) {
	define('DB_PASS', '');
}
if (!defined('DB_NAME')) {
	define('DB_NAME', '');
}


