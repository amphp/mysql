<?php

namespace Amp\Mysql\Internal;

use Amp\AsyncGenerator;
use Amp\Deferred;
use Amp\DisposedException;
use Amp\Mysql\Result;
use Amp\Promise;
use Amp\Stream;
use Amp\Success;
use function Amp\call;

final class ConnectionResult implements Result
{
    /** @var ResultProxy */
    private $result;

    /** @var AsyncGenerator */
    private $generator;

    /** @var Promise|null */
    private $nextResult;

    public function __construct(ResultProxy $result)
    {
        $this->result = $result;
        $this->generator = self::makeStream($result);
    }

    private static function makeStream(ResultProxy $result): Stream
    {
        return new AsyncGenerator(static function (callable $emit) use ($result): \Generator {
            $next = self::fetchRow($result);
            try {
                while ($row = yield $next) {
                    if (!isset($columnNames)) {
                        $columnNames = \array_column($result->columns, 'name');
                    }
                    $next = self::fetchRow($result);
                    yield $emit(\array_combine($columnNames, $row));
                }
            } catch (DisposedException $exception) {
                // Discard remaining results if disposed.
                while ($row = yield $next) {
                    $next = self::fetchRow($result);
                }
            }
        });
    }

    /**
     * @inheritDoc
     */
    public function continue(): Promise
    {
        return $this->generator->continue();
    }

    public function dispose(): void
    {
        $this->generator->dispose();
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
     * @return Promise<bool> Resolves with true if another result set exists, false if all result sets have
     *     been consumed.
     */
    public function getNextResult(): Promise
    {
        if ($this->nextResult) {
            return $this->nextResult;
        }

        return $this->nextResult = call(function (): \Generator {
            $deferred = $this->result->next ?: $this->result->next = new Deferred;
            $result = yield $deferred->promise();

            if ($result instanceof ResultProxy) {
                return new self($result);
            }

            return $result; // Instance of CommandResult or null.
        });
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
     * @return Promise<mixed[][]>
     */
    public function getFields(): Promise
    {
        if ($this->result === null) {
            throw new \Error("The current result set is empty; call this method before invoking ResultSet::nextResultSet()");
        }

        if ($this->result->state >= ResultProxy::COLUMNS_FETCHED) {
            return new Success($this->result->columns);
        }

        $deferred = new Deferred;
        $this->result->deferreds[ResultProxy::COLUMNS_FETCHED][] = [$deferred, &$this->result->columns, null];
        return $deferred->promise();
    }
}
