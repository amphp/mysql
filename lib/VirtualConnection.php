<?php

namespace Mysql;

class VirtualConnection {
	private $call = [];
	public $connRef; /* ref! */

	public function getCall() {
		$cur = current($this->call);
		if ($cur) {
			unset($this->call[key($this->call)]);
			return $cur;
		}
		return NULL;
	}

	public function __call($func, $args) {
		$this->call[] = [&$this->connRef, $func, $args];
	}
}