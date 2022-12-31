<?php declare(strict_types=1);

namespace Amp\Mysql\Internal;

use Amp\Future;
use Amp\Mysql\MysqlResult;
use Amp\Sql\Common\CommandResult;

/**
 * @internal
 * @psalm-import-type TRow from MysqlResult
 * @implements \IteratorAggregate<int, never>
 */
final class MysqlCommandResult implements MysqlResult, \IteratorAggregate
{
    private ?int $lastInsertId;

    /** @var CommandResult<TRow, MysqlResult> */
    private readonly CommandResult $delegate;

    public function __construct(int $affectedRows, int $lastInsertId)
    {
        /** @var Future<MysqlResult|null> $future Explicit declaration for Psalm. */
        $future = Future::complete();

        $this->delegate = new CommandResult($affectedRows, $future);
        $this->lastInsertId = $lastInsertId ?: null; // Convert 0 to null
    }

    public function getIterator(): \Traversable
    {
        return $this->delegate->getIterator();
    }

    public function fetchRow(): ?array
    {
        return $this->delegate->fetchRow();
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
