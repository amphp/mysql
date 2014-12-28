<?php

namespace Mysql;

use Amp\Future;
use Amp\Reactor;

class VirtualConnection {
	private $call = [];

	public function getCall() {
		$cur = current($this->call);
		if ($cur) {
			unset($this->call[key($this->call)]);
			return $cur;
		}
		return NULL;
	}

	public function fail($e) {
		foreach ($this->call as list(, $args)) {
			end($args)->fail($e);
		}
		$this->call = [];
	}

	public function __call($func, $args) {
		$future = new Future;
		$this->call[] = [$func, array_merge($args, [$future])];
		return $future;
	}
}