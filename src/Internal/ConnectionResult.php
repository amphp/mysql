<?php

namespace Amp\Mysql\Internal;

use Amp\Deferred;
use Amp\Future;
use Amp\Mysql\Result;
use Revolt\EventLoop;
use function Amp\coroutine;

final class ConnectionResult implements Result, \IteratorAggregate
{
    private ResultProxy $result;

    private \Generator $generator;

    private ?Future $nextResult = null;

    public function __construct(ResultProxy $result)
    {
        $this->result = $result;
        $this->generator = self::iterate($result);
    }

    public function getIterator(): \Traversable
    {
        return $this->generator;
    }

    private static function iterate(ResultProxy $result): \Generator
    {
        $next = self::fetchRow($result);

        try {
            while ($row = $next->await()) {
                if (!isset($columnNames)) {
                    $columnNames = \array_column($result->columns, 'name');
                }
                $next = self::fetchRow($result);
                yield \array_combine($columnNames, $row);
            }
        } finally {
            if (($row ?? null) === null) {
                return; // Result fully consumed.
            }

            EventLoop::queue(static function () use ($next, $result): void {
                try {
                    // Discard remaining results if disposed.
                    while ($next->await()) {
                        $next = self::fetchRow($result);
                    }
                } catch (\Throwable) {
                    // Ignore errors while discarding result.
                }
            });
        }

    }

    private static function fetchRow(ResultProxy $result): Future
    {
        if ($result->userFetched < $result->fetchedRows) {
            $row = $result->rows[$result->userFetched];
            unset($result->rows[$result->userFetched]);
            $result->userFetched++;
            return Future::complete($row);
        }

        if ($result->state === ResultProxy::ROWS_FETCHED) {
            return Future::complete(null);
        }

        $deferred = new Deferred;

        /* We need to increment the internal counter, else the next time fetch is called,
         * it'll simply return the row we fetch here instead of fetching a new row
         * since callback order on promises isn't defined, we can't do this via onResolve() */
        $incRow = function ($row) use ($result) {
            unset($result->rows[$result->userFetched++]);
            return $row;
        };

        $result->deferreds[ResultProxy::UNFETCHED][] = [$deferred, null, $incRow];
        return $deferred->getFuture();
    }

    /**
     * @inheritDoc
     */
    public function getNextResult(): ?Result
    {
        if ($this->nextResult) {
            return $this->nextResult->await();
        }

        $this->nextResult = coroutine(function (): ?Result {
            $deferred = $this->result->next ?: $this->result->next = new Deferred;
            $result = $deferred->getFuture()->await();

            if ($result instanceof ResultProxy) {
                return new self($result);
            }

            return $result; // Instance of CommandResult or null.
        });

        return $this->nextResult->await();
    }

    public function getRowCount(): ?int
    {
        return $this->result->affectedRows;
    }

    public function getColumnCount(): ?int
    {
        return $this->result->columnCount;
    }

    public function getLastInsertId(): ?int
    {
        return $this->result->insertId;
    }

    /**
     * @inheritDoc
     */
    public function getFields(): ?array
    {
        if ($this->result === null) {
            throw new \Error("The current result set is empty; call this method before invoking ResultSet::nextResultSet()");
        }

        if ($this->result->state >= ResultProxy::COLUMNS_FETCHED) {
            return $this->result->columns;
        }

        $deferred = new Deferred;
        $this->result->deferreds[ResultProxy::COLUMNS_FETCHED][] = [$deferred, &$this->result->columns, null];
        return $deferred->getFuture()->await();
    }
}
