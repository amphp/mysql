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

    /** @var \Amp\Mysql\Internal\ResultProxy */
    private $result;

    /** @var \Amp\Producer */
    private $producer;

    /** @var \Amp\Mysql\Internal\CompletionQueue */
    private $queue;

    /** @var array|object Last emitted row. */
    private $currentRow;

    /** @var int Fetch type of next row. */
    private $type;

    public function __construct(Internal\ResultProxy $result) {
        $this->result = $result;
        $queue = $this->queue = new Internal\CompletionQueue;

        $last = &$this->result;
        $this->producer = new Producer(static function (callable $emit) use (&$last, $result, $queue) {
            try {
                do {
                    $last = $result;
                    $row = yield self::fetch($result);
                    while ($row !== null) {
                        $next = self::fetch($result); // Fetch next row while emitting last row.
                        yield $emit($row);
                        $row = yield $next;
                    }
                } while ($result = yield self::getNextResultSet($result));
            } finally {
                $queue->complete();
            }
        });
    }

    public function __destruct() {
        if (!$this->queue->isComplete()) { // Producer above did not complete, so consume remaining results.
            Promise\rethrow(new Coroutine($this->dispose()));
        }
    }

    private function dispose(): \Generator {
        try {
            while (yield $this->producer->advance()); // Discard unused result rows.
        } catch (\Throwable $exception) {
            // Ignore failure while discarding results.
        }
    }

    /**
     * {@inheritdoc}
     */
    public function onComplete(callable $onComplete) {
        $this->queue->onComplete($onComplete);
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

        switch ($this->type) {
            case self::FETCH_ASSOC:
                return $this->currentRow = \array_combine(
                    \array_column($this->result->columns, "name"), $this->producer->getCurrent()
                );
            case self::FETCH_ARRAY:
                return $this->currentRow = $this->producer->getCurrent();
            case self::FETCH_OBJECT:
                return $this->currentRow = (object) \array_combine(
                    \array_column($this->result->columns, "name"), $this->producer->getCurrent()
                );
            default:
                throw new \Error("Invalid result fetch type");
        }
    }

    private static function fetch(Internal\ResultProxy $result): Promise {
        if ($result->userFetched < $result->fetchedRows) {
            $row = $result->rows[$result->userFetched++];
            return new Success($row);
        } elseif ($result->state == Internal\ResultProxy::ROWS_FETCHED) {
            return new Success(null);
        } else {
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
    }

    /**
     * @return \Amp\Promise<\Amp\Mysql\Internal\ResultProxy|null>
     */
    private static function getNextResultSet(Internal\ResultProxy $result): Promise {
        $deferred = $result->next ?: $result->next = new Deferred;
        return $deferred->promise();
    }

    /**
     * @return \Amp\Promise<mixed[][]>
     */
    public function getFields(): Promise {
        if ($this->result->state >= Internal\ResultProxy::COLUMNS_FETCHED) {
            return new Success($this->result->columns);
        } else {
            $deferred = new Deferred;
            $this->result->deferreds[Internal\ResultProxy::COLUMNS_FETCHED][] = [$deferred, &$this->result->columns, null];
            return $deferred->promise();
        }
    }
}
