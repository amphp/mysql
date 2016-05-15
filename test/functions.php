<?php
const DB_HOST = 'localhost:10101';
const DB_USER = 'root';
const DB_PASS = '';

//this allows us to override the commands used (eg: for testing different
//versions of mysql/mariadb)
@include_once __DIR__ . "../testdb_config.php";

if (!defined("MYSQL_BASEDIR")) {
	//mysql_install_db requires --basedir to find other commands
	//if mysql is installed to /usr/bin, then PREFIX must be /usr
	$basedir = dirname(dirname(execCommandOrDie("which mysql")));
	if (!$basedir) {
		print "Unable to locate mysql prefix\n";
		exit(-1);
	}
	define("MYSQL_BASEDIR", $basedir);
}

define("MYSQL_CFG_PATH", __DIR__ . DIRECTORY_SEPARATOR . "my.cnf");

//returns the given command using the configured mysql prefix, along with
//--defaults-file=MYSQL_CFG_PATH added to args
function mysqlCmd($cmdName, $args="") {
	$fullcmd = implode(DIRECTORY_SEPARATOR, [MYSQL_BASEDIR, "bin", $cmdName]);

	return "$fullcmd --defaults-file='" . MYSQL_CFG_PATH . "' " . $args;
}

function startMySQLdOrDie() {
	$pipes = [];

	$proc = proc_open(mysqlCmd("mysqld"), [2 => ["pipe", "w"]], $pipes, __DIR__);
	$stderr = $pipes[2];
	$buf = "";
	do {
		if (!($row = fgets($stderr)) || preg_match("# \[ERROR\] #", $row)) {
			print "\nERROR: Aborting, couldn't start mysql successfully\n$buf$row";
			exit(-1);
		}
		$buf .= $row;
	} while (!preg_match("(^Version: '[0-9.a-zA-Z]+)", $row));

	register_shutdown_function(function() use ($proc) {
		proc_terminate($proc);
	});
}

function execCommandOrDie($cmd) {
	$l = exec($cmd, $output, $retval);
	if ($retval) {
		print "Command `$cmd` failed:\n";
		print implode("\n", $output);
		print "\n";
		exit(-1);
	}
	return $l;
}

?>
