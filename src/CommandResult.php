<?php

namespace Amp\Mysql;

use Amp\Sql\CommandResult as SqlCommandResult;

final class CommandResult implements SqlCommandResult
{
    /** @var int */
    private $affectedRows;

    /** @var int */
    private $lastInsertId;

    public function __construct(int $affectedRows, int $lastInsertId)
    {
        $this->affectedRows = $affectedRows;
        $this->lastInsertId = $lastInsertId;
    }

    /**
     * @return int Number of rows affected by the modification query.
     */
    public function getAffectedRowCount(): int
    {
        return $this->affectedRows;
    }

    /**
     * @return int Insert ID of the last auto increment row.
     */
    public function getLastInsertId(): int
    {
        return $this->lastInsertId;
    }
}
