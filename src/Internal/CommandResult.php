<?php

namespace Amp\Mysql\Internal;

use Amp\Future;
use Amp\Mysql\Result;
use Amp\Sql\Common\CommandResult as SqlCommandResult;

final class CommandResult implements Result, \IteratorAggregate
{
    private ?int $lastInsertId;

    private SqlCommandResult $delegate;

    public function __construct(int $affectedRows, int $lastInsertId)
    {
        $this->delegate = new SqlCommandResult($affectedRows, Future::complete(null));
        $this->lastInsertId = $lastInsertId ?: null; // Convert 0 to null
    }

    public function getIterator(): \Traversable
    {
        return $this->delegate->getIterator();
    }

    public function getRowCount(): int
    {
        return $this->delegate->getRowCount();
    }

    /**
     * @return int|null Insert ID of the last auto increment row.
     */
    public function getLastInsertId(): ?int
    {
        return $this->lastInsertId;
    }

    public function getNextResult(): ?Result
    {
        return $this->delegate->getNextResult();
    }

    public function getFields(): ?array
    {
        return null; // Command results do not have a field list.
    }
}
