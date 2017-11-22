<?php

namespace Amp\Mysql;

class CommandResult {
    /** @var int */
    private $affectedRows;

    /** @var int */
    private $insertId;

    public function __construct(int $affectedRows, int $insertId) {
        $this->affectedRows = $affectedRows;
        $this->insertId = $insertId;
    }

    /**
     * @return int Number of rows affected by the modification query.
     */
    public function affectedRows(): int {
        return $this->affectedRows;
    }

    /**
     * @return int Insert ID of the last auto increment row.
     */
    public function insertId(): int {
        return $this->insertId;
    }
}
