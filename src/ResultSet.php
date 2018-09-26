<?php

namespace Amp\Mysql;

use Amp\Promise;
use Amp\Sql\ResultSet as SqlResultSet;

interface ResultSet extends SqlResultSet
{
    /**
     * @return Promise<bool> Resolves with true if another result set exists, false if all result sets have
     *     been consumed.
     */
    public function nextResultSet(): Promise;

    /**
     * @return Promise<mixed[][]>
     *
     * @throws \Error If nextResultSet() has been invoked and no further result sets were available.
     */
    public function getFields(): Promise;
}
