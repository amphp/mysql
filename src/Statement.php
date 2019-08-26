<?php

namespace Amp\Mysql;

use Amp\Promise;
use Amp\Sql\Statement as SqlStatement;

interface Statement extends SqlStatement
{
    /**
     * @param int|string $paramId Parameter ID or name.
     * @param mixed $data Data to bind to parameter.
     *
     * @throws \Error If $paramId is not an int or string, or the position does not exist.
     */
    public function bind($paramId, $data): void;

    /**
     * @return Promise<array>
     */
    public function getFields(): Promise;

    /**
     * Reset statement to state just after preparing.
     *
     * @return Promise<null>
     */
    public function reset(): Promise;
}
