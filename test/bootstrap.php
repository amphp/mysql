<?php

namespace Amp\Mysql\Test;

use Amp\ByteStream;
use Amp\Process\Process;
use function Amp\delay;

require __DIR__.'/../vendor/autoload.php';

const DB_HOST = 'localhost:10101';
const DB_USER = 'root';
const DB_PASS = '';
const DB_NAME = 'test';

print "\rCleaning up test directory...";

(function (): void {
    /* cleanup in case it wasn't terminated properly... */
    $pidfile = __DIR__ . "/mysql.pid";
    if (\file_exists($pidfile)) {
        if (\stripos(PHP_OS, "win") === 0) {
            \shell_exec("Taskkill /PID " . \file_get_contents($pidfile) . " /F");
        } else {
            \shell_exec("kill -9 `cat '$pidfile'` 2>/dev/null");
        }
        delay(1);
    }

    if (\file_exists(__DIR__ . "/mysql_db")) {
        $rm_all = function ($dir) use (&$rm_all): void {
            $files = \glob("$dir/*");
            if (\is_array($files)) {
                foreach ($files as $file) {
                    if (\is_dir($file)) {
                        $rm_all($file);
                        \rmdir($file);
                    } else {
                        \unlink($file);
                    }
                }
            }
        };
        $rm_all(__DIR__ . "/mysql_db");
    } else {
        @\mkdir(__DIR__ . "/mysql_db");
    }
})();

print "\rCreating mysql server...     ";

$dir = __DIR__;

$process = new Process("mysqld --defaults-file={$dir}/my.cnf --initialize-insecure", __DIR__);

$process->start();

$stderr = ByteStream\buffer($process->getStderr());

if (\preg_match("# \[ERROR\] #", $stderr)) {
    print "\nERROR: Aborting, couldn't start mysql successfully\n{$stderr}";
    exit(127);
}

$process->join();

$process = new Process("mysqld --defaults-file={$dir}/my.cnf --user=root", __DIR__);

$process->start();

print "\rStarting mysqld...           ";

delay(2); // Give mysqld time to start.

print "\rCreating test database...    ";

$db = new \mysqli(DB_HOST, DB_USER, DB_PASS);
$db->query("CREATE DATABASE test");
$db->query("CREATE TABLE test.main (id INT NOT NULL AUTO_INCREMENT, a INT, b INT, PRIMARY KEY (id))");
$db->query("INSERT INTO test.main (a, b) VALUES (1, 2), (2, 3), (3, 4), (4, 5), (5, 6)");
$db->close();

print "\r";

\register_shutdown_function(fn() => $process->signal(\SIGTERM));
