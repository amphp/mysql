<?php

namespace Mysql;

class ConnectionConfig {
	public $ready = null;
	public $exceptions = true;
	public $restore;
	public $charset = 0x21; // utf8_general_ci
}