<?php

namespace Amp\Mysql\Internal;

use Amp\Loop;

class ReferenceQueue {
    /** @var bool */
    private $done = false;

    /** @var callable[] */
    private $onDestruct = [];

    /** @var int */
    private $refCount = 1;

    public function onDestruct(callable $onDestruct) {
        if ($this->done) {
            $onDestruct();
            return;
        }

        $this->onDestruct[] = $onDestruct;
    }

    public function reference() {
        ++$this->refCount;
    }

    public function unreference() {
        if ($this->done) {
            return;
        }

        if (--$this->refCount) {
            return;
        }

        $this->done = true;
        foreach ($this->onDestruct as $callback) {
            try {
                $callback();
            } catch (\Throwable $exception) {
                Loop::defer(function () use ($exception) {
                    throw $exception; // Rethrow to event loop error handler.
                });
            }
        }
        $this->onDestruct = null;
    }

    public function isReferenced(): bool {
        return $this->done;
    }
}
