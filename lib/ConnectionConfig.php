<?php

namespace Mysql;

class ConnectionConfig {
	public $ready = null;
	public $busy = null;
	public $exceptions = true;
	public $restore = null;
	public $charset = 0x21; // utf8_general_ci
}