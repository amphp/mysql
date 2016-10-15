<?php

$autoloader = require(__DIR__.'/../vendor/autoload.php');

const DB_HOST = 'localhost:10101';
const DB_USER = 'root';
const DB_PASS = '';

$pipes = [];
/* cleanup in case it wasn't terminated properly... */
$pidfile = __DIR__."/mysql.pid";
if (file_exists($pidfile)) {
	if (stripos(PHP_OS, "win") === 0) {
		shell_exec("Taskkill /PID ".file_get_contents($pidfile)." /F");
	} else {
		shell_exec("kill -9 `cat '$pidfile'`");
	}
	sleep(1);
}
if (file_exists(__DIR__."/mysql_db")) {
	$rm_all = function ($dir) use (&$rm_all) {
		$files = glob("$dir/*");
		if (is_array($files)) {
			foreach ($files as $file) {
				if (is_dir($file)) {
					$rm_all($file);
					rmdir($file);
				} else {
					unlink($file);
				}
			}
		}
	};
	$rm_all(__DIR__ . "/mysql_db");
} else {
	@mkdir(__DIR__ . "/mysql_db");
}

$proc = proc_open("mysqld --defaults-file=my.cnf --initialize-insecure", [2 => ["pipe", "w"]], $pipes, __DIR__);
$stderr = $pipes[2];
do {
	if (!($row = fgets($stderr)) || preg_match("# \[ERROR\] #", $row)) {
		print "\nERROR: Aborting, couldn't start mysql successfully\n$buf$row";
		exit(127);
	}
	$buf .= $row;
} while (!preg_match("(root@localhost is created with an empty password)", $row));
sleep(3); // :-(
proc_terminate($proc, 9);
@proc_terminate($proc, 9);

$proc = proc_open("mysqld --defaults-file=my.cnf --user=root", [2 => ["pipe", "w"]], $pipes, __DIR__);

register_shutdown_function(function() use ($proc) {
	proc_terminate($proc, 9);
});

$stderr = $pipes[2];
do {
	if (!($row = fgets($stderr)) || preg_match("# \[ERROR\] #", $row)) {
		print "\nERROR: Aborting, couldn't start mysql successfully\n$buf$row";
		exit(127);
	}
	$buf .= $row;
} while (!preg_match("(^Version: '[0-9.a-zA-Z]+')", $row));


$db = new mysqli(DB_HOST, DB_USER, DB_PASS);
$db->query("CREATE DATABASE connectiontest");
$db->query("CREATE TABLE connectiontest.main SELECT 1 AS a, 2 AS b");
$db->query("INSERT INTO connectiontest.main VALUES (5, 6)");
$db->close();