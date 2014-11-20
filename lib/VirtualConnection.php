<?php

namespace Mysql;

use Amp\Future;
use Amp\Reactor;

class VirtualConnection {
	private $call = [];
	private $reactor;

	public function __construct(Reactor $reactor) {
		$this->reactor = $reactor;
	}

	public function getCall() {
		$cur = current($this->call);
		if ($cur) {
			unset($this->call[key($this->call)]);
			return $cur;
		}
		return NULL;
	}

	public function __call($func, $args) {
		$future = new Future($this->reactor);
		$this->call[] = [$func, array_merge($args, [$future])];
		return $future;
	}
}