<?php

namespace Amp\Mysql\Internal;

use Amp\Loop;

class CompletionQueue {
    /** @var bool */
    private $complete = false;

    /** @var callable[] */
    private $onComplete = [];

    public function onComplete(callable $onComplete) {
        if ($this->complete) {
            $onComplete();
            return;
        }

        $this->onComplete[] = $onComplete;
    }

    public function complete() {
        if ($this->complete) {
            return;
        }

        $this->complete = true;
        foreach ($this->onComplete as $callback) {
            try {
                $callback();
            } catch (\Throwable $exception) {
                Loop::defer(function () use ($exception) {
                    throw $exception; // Rethrow to event loop error handler.
                });
            }
        }
        $this->onComplete = null;
    }

    public function isComplete(): bool {
        return $this->complete;
    }
}
