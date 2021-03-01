<?php

namespace Amp\Mysql\Internal;

use Amp\AsyncGenerator;
use Amp\Deferred;
use Amp\Mysql\Result;
use Amp\Promise;
use Amp\Success;
use function Amp\async;
use function Amp\await;
use function Amp\defer;

final class ConnectionResult implements Result, \IteratorAggregate
{
    private ResultProxy $result;

    private AsyncGenerator $generator;

    private ?Promise $nextResult = null;

    public function __construct(ResultProxy $result)
    {
        $this->result = $result;
        $this->generator = new AsyncGenerator(static function () use ($result): \Generator {
            $next = self::fetchRow($result);
            try {
                while ($row = await($next)) {
                    if (!isset($columnNames)) {
                        $columnNames = \array_column($result->columns, 'name');
                    }
                    $next = self::fetchRow($result);
                    yield \array_combine($columnNames, $row);
                }
            } finally {
                if ($row === null) {
                    return; // Result fully consumed.
                }

                defer(static function () use ($next, $result): void {
                    try {
                        // Discard remaining results if disposed.
                        while ($row = await($next)) {
                            $next = self::fetchRow($result);
                        }
                    } catch (\Throwable $exception) {
                        // Ignore errors while discarding result.
                    }
                });
            }
        });
    }

    /**
     * @inheritDoc
     */
    public function continue(): ?array
    {
        return $this->generator->continue();
    }

    public function dispose(): void
    {
        $this->generator->dispose();
    }

    public function getIterator(): \Traversable
    {
        return $this->generator->getIterator();
    }

    private static function fetchRow(ResultProxy $result): Promise
    {
        if ($result->userFetched < $result->fetchedRows) {
            $row = $result->rows[$result->userFetched];
            unset($result->rows[$result->userFetched]);
            $result->userFetched++;
            return new Success($row);
        }

        if ($result->state === ResultProxy::ROWS_FETCHED) {
            return new Success;
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
        return $deferred->promise();
    }

    /**
     * @inheritDoc
     */
    public function getNextResult(): ?Result
    {
        if ($this->nextResult) {
            return await($this->nextResult);
        }

        $this->nextResult = async(function (): ?Result {
            $deferred = $this->result->next ?: $this->result->next = new Deferred;
            $result = await($deferred->promise());

            if ($result instanceof ResultProxy) {
                return new self($result);
            }

            return $result; // Instance of CommandResult or null.
        });

        return await($this->nextResult);
    }

    public function getRowCount(): ?int
    {
        return $this->result->affectedRows;
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
        return await($deferred->promise());
    }
}
