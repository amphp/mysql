<?php

namespace Amp\Mysql;

use Amp\Sql\Result as SqlResult;

interface Result extends SqlResult
{
    /**
     * @return int|null Insert ID of the last auto increment row if applicable to the result or null if no ID
     *                  is available.
     */
    public function getLastInsertId(): ?int;

    /**
     * @psalm-return array<int, array<int, string>>|null
     */
    public function getFields(): ?array;
}
