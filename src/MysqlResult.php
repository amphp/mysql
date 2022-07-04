<?php

namespace Amp\Mysql;

use Amp\Sql\Result;

interface MysqlResult extends Result
{
    /**
     * @return int|null Insert ID of the last auto increment row if applicable to the result or null if no ID
     *                  is available.
     */
    public function getLastInsertId(): ?int;

    /**
     * @return list<MysqlColumnDefinition>|null
     */
    public function getColumnDefinitions(): ?array;
}
