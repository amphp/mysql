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
	public $binCharset = 45; // utf8mb4_general_ci
	public $charset = "utf8mb4";
	public $collate = "utf8mb4_general_ci";
	public $key = [];
}