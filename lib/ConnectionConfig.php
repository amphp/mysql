<?php

namespace Mysql;

class ConnectionConfig {
	public $host;
	public $resolvedHost;
	public $user;
	public $pass;
	public $db = null;
	public $ssl = null;

	public $ready = null;
	public $busy = null;
	public $exceptions = true;
	public $restore = null;
	public $binCharset = 0x21; // utf8_general_ci
	public $charset = "utf8";
	public $collate = "utf8_general_ci";
	public $key = [];
}