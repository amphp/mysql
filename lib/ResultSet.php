<?php

namespace Amp\Mysql;

use Amp\Coroutine;
use Amp\Deferred;
use Amp\Iterator;
use Amp\Producer;
use Amp\Promise;
use Amp\Success;

class ResultSet implements Iterator, Operation {
    const FETCH_ARRAY = 0;
    const FETCH_ASSOC = 1;
    const FETCH_OBJECT = 2;

    /** @var \Amp\Mysql\Internal\ResultProxy|null */
    private $result;

    /** @var \Amp\Producer */
    private $producer;

    /** @var \Amp\Mysql\Internal\ReferenceQueue */
    private $queue;

    /** @var array|object Last emitted row. */
    private $currentRow;

    /** @var int Fetch type of next row. */
    private $type;

    /** @var string[]|null */
    private $columnNames;

    public function __construct(Internal\ResultProxy $result) {
        $this->result = $result;
        $this->queue = new Internal\ReferenceQueue;
        $this->producer = self::makeIterator($result);
    }

    private static function makeIterator(Internal\ResultProxy $result): Iterator {
        return new Producer(static function (callable $emit) use ($result) {
            $row = yield self::fetchRow($result);
            while ($row !== null) {
                $next = self::fetchRow($result); // Fetch next row while emitting last row.
                yield $emit($row);
                $row = yield $next;
            }
        });
    }

    public function __destruct() {
        if ($this->result) { // All results were not necessarily consumed.
            Promise\rethrow(new Coroutine($this->dispose()));
        }
    }

    private function dispose(): \Generator {
        try {
            do {
                while (yield $this->advance()); // Discard unused result rows.
            } while (yield $this->nextResultSet());
        } catch (\Throwable $exception) {
            // Ignore failure while discarding results.
            $this->queue->unreference();
        }
    }

    /**
     * {@inheritdoc}
     */
    public function onDestruct(callable $onDestruct) {
        $this->queue->onDestruct($onDestruct);
    }

    /**
     * {@inheritdoc}
     *
     * @param int $type Result fetch type. Use the FETCH_* constant defined by this class.
     */
    public function advance(int $type = self::FETCH_ASSOC): Promise {
        $this->currentRow = null;
        $this->type = $type;

        return $this->producer->advance();
    }

    /**
     * {@inheritdoc}
     */
    public function getCurrent() {
        if ($this->currentRow !== null) {
            return $this->currentRow;
        }

        $row = $this->producer->getCurrent();

        if (!$this->columnNames) {
            $this->columnNames = \array_column($this->result->columns, "name");
        }

        switch ($this->type) {
            case self::FETCH_ASSOC:
                return $this->currentRow = \array_combine($this->columnNames, $row);

            case self::FETCH_ARRAY:
                return $this->currentRow = $row;

            case self::FETCH_OBJECT:
                return $this->currentRow = (object) \array_combine($this->columnNames, $row);

            default:
                throw new \Error("Invalid result fetch type");
        }
    }

    private static function fetchRow(Internal\ResultProxy $result): Promise {
        if ($result->userFetched < $result->fetchedRows) {
            return new Success($result->rows[$result->userFetched++]);
        }

        if ($result->state === Internal\ResultProxy::ROWS_FETCHED) {
            return new Success;
        }

        $deferred = new Deferred;

        /* We need to increment the internal counter, else the next time fetch is called,
         * it'll simply return the row we fetch here instead of fetching a new row
         * since callback order on promises isn't defined, we can't do this via onResolve() */
        $incRow = function ($row) use ($result) {
            $result->userFetched++;
            return $row;
        };

        $result->deferreds[Internal\ResultProxy::SINGLE_ROW_FETCH][] = [$deferred, null, $incRow];
        return $deferred->promise();
    }

    /**
     * @return \Amp\Promise<bool> Resolves with true if another result set exists, false if all result sets have
     *     been consumed.
     */
    public function nextResultSet(): Promise {
        if (!$this->result) {
            return new Success(false);
        }

        return \Amp\call(function () {
            while (yield $this->advance()); // Consume any values left in the current result.

            $this->columnNames = null;

            $deferred = $this->result->next ?: $this->result->next = new Deferred;
            $this->result = yield $deferred->promise();

            if ($this->result) {
                $this->producer = self::makeIterator($this->result);
                return true;
            }

            $this->queue->unreference();
            return false;
        });
    }

    /**
     * @return \Amp\Promise<mixed[][]>
     *
     * @throws \Error If nextResultSet() has been invoked and no further result sets were available.
     */
    public function getFields(): Promise {
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
