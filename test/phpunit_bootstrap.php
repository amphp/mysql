<?php

$autoloader = require(__DIR__.'/../vendor/autoload.php');
require_once 'functions.php';

/* cleanup in case it wasn't terminated properly... */
$pidfile = __DIR__."/mysql.pid";
if (file_exists($pidfile)) {
	shell_exec("kill `cat '$pidfile'`");
	sleep(1);
}
startMySQLdOrDie();
