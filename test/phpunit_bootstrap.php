<?php

$autoloader = require(__DIR__.'/../vendor/autoload.php');

const DB_HOST = 'localhost';
const DB_USER = 'root';
const DB_PASS = '';

$pipes = [];
/* cleanup in case it wasn't terminated properly... */
$pidfile = __DIR__."/mysql.pid";
if (file_exists($pidfile)) {
	shell_exec("kill `cat '$pidfile'`");
	sleep(1);
}
$proc = proc_open("mysqld --defaults-file=my.cnf", [2 => ["pipe", "w"]], $pipes, __DIR__);
$stderr = $pipes[2];
$buf = "";
do {
	if (!($row = fgets($stderr)) || preg_match("# \[ERROR\] #", $row)) {
		die("\nERROR: Aborting, couldn't start mysql successfully\n$buf$row");
	}
	$buf .= $row;
} while (!preg_match("(^Version: '[0-9.a-zA-Z]+')", $row));

register_shutdown_function(function() use ($proc) {
	proc_terminate($proc);
});