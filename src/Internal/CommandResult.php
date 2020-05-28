<?php

namespace Amp\Mysql\Internal;

use Amp\Mysql\Result;
use Amp\Promise;
use Amp\Sql\Common\CommandResult as SqlCommandResult;
use Amp\Success;

final class CommandResult implements Result
{
    /** @var int */
    private $lastInsertId;

    /** @var SqlCommandResult */
    private $delegate;

    public function __construct(int $affectedRows, int $lastInsertId)
    {
        $this->delegate = new SqlCommandResult($affectedRows, new Success);
        $this->lastInsertId = $lastInsertId ?: null; // Convert 0 to null
    }

    public function continue(): Promise
    {
        return $this->delegate->continue();
    }

    public function dispose()
    {
        $this->delegate->dispose();
    }

    public function getRowCount(): int
    {
        return $this->delegate->getRowCount();
    }

    /**
     * @return int Insert ID of the last auto increment row.
     */
    public function getLastInsertId(): ?int
    {
        return $this->lastInsertId;
    }

    public function getNextResult(): Promise
    {
        return $this->delegate->getNextResult();
    }

    public function getFields(): Promise
    {
        return new Success; // Command results do not have a field list.
    }
}
