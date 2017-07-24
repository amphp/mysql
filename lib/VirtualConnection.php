<?php

namespace Amp\Mysql;

use Amp\Deferred;

class VirtualConnection {
    private $call = [];

    public function getCall() {
        $cur = current($this->call);
        if ($cur) {
            unset($this->call[key($this->call)]);
            return $cur;
        }
        return null;
    }

    public function fail($e) {
        foreach ($this->call as list($deferred)) {
            $deferred->fail($e);
        }
        $this->call = [];
    }

    public function __call($func, $args) {
        $deferred = new Deferred;
        $this->call[] = [$deferred, $func, $args];
        return $deferred->promise();
    }
}