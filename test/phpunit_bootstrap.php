<?php


$autoloader = require(__DIR__.'/../vendor/autoload.php');

// Add a PSR-0 classpath for testing
// $autoloader->add('Example', [realpath('./').'/test/']);

// Add a PSR-4 classpath for testing.
// $autoloader->addPsr4('Example', [realpath('./').'/test/']);


@include_once "./mysql_config.php";

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
