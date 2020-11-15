<?php

namespace Amp\Mysql;

use Amp\Sql\Statement as SqlStatement;

interface Statement extends SqlStatement
{
    public function execute(array $params = []): Result;

    /**
     * @param int|string $paramId Parameter ID or name.
     * @param mixed $data Data to bind to parameter.
     *
     * @throws \Error If $paramId is not an int or string, or the position does not exist.
     */
    public function bind(int|string $paramId, mixed $data): void;

    public function getFields(): ?array;

    /**
     * Reset statement to state just after preparing.
     */
    public function reset(): void;
}
