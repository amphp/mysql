<?php

namespace Amp\Mysql;

interface Operation {
    /**
     * @param callable $onDestruct Callback executed when the operation completes or the object is destroyed.
     */
    public function onDestruct(callable $onDestruct);
}
