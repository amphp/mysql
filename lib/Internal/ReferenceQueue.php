<?php

namespace Amp\Mysql\Internal;

use Amp\Loop;

class ReferenceQueue {
    /** @var callable[] */
    private $onDestruct = [];

    /** @var int */
    private $refCount = 1;

    public function onDestruct(callable $onDestruct) {
        if (!$this->refCount) {
            try {
                $onDestruct();
            } catch (\Throwable $exception) {
                Loop::defer(function () use ($exception) {
                    throw $exception; // Rethrow to event loop error handler.
                });
            }
            return;
        }

        $this->onDestruct[] = $onDestruct;
    }

    public function reference() {
        \assert($this->refCount, "The reference queue has already been fully unreferenced and destroyed");
        ++$this->refCount;
    }

    public function unreference() {
        \assert($this->refCount, "The reference queue has already been fully unreferenced and destroyed");

        if (--$this->refCount) {
            return;
        }

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
        return (bool) $this->refCount;
    }
}
