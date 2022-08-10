<?php

namespace Amp\Mysql\Internal;

use Amp\Future;
use Amp\Mysql\MysqlResult;
use Amp\Sql\Common\CommandResult;

/** @internal */
final class MysqlCommandResult implements MysqlResult, \IteratorAggregate
{
    private ?int $lastInsertId;

    /** @var CommandResult<null> */
    private readonly CommandResult $delegate;

    public function __construct(int $affectedRows, int $lastInsertId)
    {
        $this->delegate = new CommandResult($affectedRows, Future::complete());
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

    public function getColumnCount(): ?int
    {
        return $this->delegate->getColumnCount();
    }

    /**
     * @return int|null Insert ID of the last auto increment row.
     */
    public function getLastInsertId(): ?int
    {
        return $this->lastInsertId;
    }

    public function getNextResult(): ?MysqlResult
    {
        return $this->delegate->getNextResult();
    }

    public function getColumnDefinitions(): ?array
    {
        return null; // Command results do not have a field list.
    }
}
