<?php

$autoloader = require(__DIR__.'/../vendor/autoload.php');

define('DB_HOST', 'localhost');
define('DB_USER', 'root');
define('DB_PASS', '');

$pipes = [];
$proc = proc_open("mysqld --defaults-file=my.cnf", [2 => ["pipe", "w"]], $pipes, __DIR__);
$stderr = $pipes[2];
$buf = "";
do {
	if (!($row = fgets($stderr)) || preg_match("# \[ERROR\] #", $row)) {
		die("\nERROR: Aborting, couldn't start mysql successfully\n$buf");
	}
	$buf .= $row;
} while (preg_match("(^Version: '[0-9.a-zA-Z]+')", $row));

register_shutdown_function(function() use ($proc) {
	proc_terminate($proc);
});