<?php

namespace Amp\Mysql;

use Amp\Deferred;
use Amp\Iterator;
use Amp\Producer;
use Amp\Promise;
use Amp\Success;
use function Amp\asyncCall;
use function Amp\call;

final class ConnectionResultSet implements ResultSet
{
    /** @var Internal\ResultProxy|null */
    private $result;

    /** @var \Amp\Producer */
    private $producer;

    /** @var array|object Last emitted row. */
    private $currentRow;

    /** @var string[]|null */
    private $columnNames;

    public function __construct(Internal\ResultProxy $result)
    {
        $this->result = $result;
        $this->producer = self::makeIterator($result);
    }

    private static function makeIterator(Internal\ResultProxy $result): Iterator
    {
        return new Producer(static function (callable $emit) use ($result) {
            $row = yield self::fetchRow($result);
            while ($row !== null) {
                $next = self::fetchRow($result); // Fetch next row while emitting last row.
                yield $emit($row);
                $row = yield $next;
            }
        });
    }

    public function __destruct()
    {
        if (!$this->result) {
            return;
        }

        $producer = $this->producer;
        asyncCall(static function () use ($producer) {
            try {
                while (yield $producer->advance());
            } catch (\Throwable $exception) {
                // Ignore iterator failure when destroying.
            }
        });
    }

    /**
     * {@inheritdoc}
     */
    public function advance(): Promise
    {
        $this->currentRow = null;

        return $this->producer->advance();
    }

    /**
     * {@inheritdoc}
     */
    public function getCurrent(): array
    {
        if ($this->currentRow !== null) {
            return $this->currentRow;
        }

        if (!$this->columnNames) {
            $this->columnNames = \array_column($this->result->columns, "name");
        }

        return $this->currentRow = \array_combine($this->columnNames, $this->producer->getCurrent());
    }

    private static function fetchRow(Internal\ResultProxy $result): Promise
    {
        if ($result->userFetched < $result->fetchedRows) {
            $row = $result->rows[$result->userFetched];
            unset($result->rows[$result->userFetched]);
            $result->userFetched++;
            return new Success($row);
        }

        if ($result->state === Internal\ResultProxy::ROWS_FETCHED) {
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

        $result->deferreds[Internal\ResultProxy::UNFETCHED][] = [$deferred, null, $incRow];
        return $deferred->promise();
    }

    /**
     * @return Promise<bool> Resolves with true if another result set exists, false if all result sets have
     *     been consumed.
     */
    public function nextResultSet(): Promise
    {
        if (!$this->result) {
            return new Success(false);
        }

        return call(function () {
            while (yield $this->advance()); // Consume any values left in the current result.

            $this->columnNames = null;

            $deferred = $this->result->next ?: $this->result->next = new Deferred;
            $this->result = yield $deferred->promise();

            if ($this->result) {
                $this->producer = self::makeIterator($this->result);
                return true;
            }

            return false;
        });
    }

    /**
     * @return Promise<mixed[][]>
     *
     * @throws \Error If nextResultSet() has been invoked and no further result sets were available.
     */
    public function getFields(): Promise
    {
        if ($this->result === null) {
            throw new \Error("The current result set is empty; call this method before invoking ResultSet::nextResultSet()");
        }

        if ($this->result->state >= Internal\ResultProxy::COLUMNS_FETCHED) {
            return new Success($this->result->columns);
        }

        $deferred = new Deferred;
        $this->result->deferreds[Internal\ResultProxy::COLUMNS_FETCHED][] = [$deferred, &$this->result->columns, null];
        return $deferred->promise();
    }
}
