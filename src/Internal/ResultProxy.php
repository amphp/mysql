<?php

namespace Amp\Mysql\Internal;

use Amp\DeferredFuture;

final class ResultProxy
{
    public int $columnCount;
    public array $columns = [];
    public array $params = [];
    public int $columnsToFetch;
    public array $rows = [];
    public int $fetchedRows = 0;
    public int $userFetched = 0;

    public ?int $insertId = null;

    public ?int $affectedRows = null;

    public array $deferreds = [
        self::UNFETCHED => [],
        self::COLUMNS_FETCHED => [],
        self::ROWS_FETCHED => []
    ];

    public int $state = self::UNFETCHED;

    public ?DeferredFuture $next = null;

    public const UNFETCHED = 0;
    public const COLUMNS_FETCHED = 1;
    public const ROWS_FETCHED = 2;

    public const SINGLE_ROW_FETCH = 255;

    public function setColumns(int $columns): void
    {
        $this->columnCount = $this->columnsToFetch = $columns;
    }

    public function updateState(int $state): void
    {
        $this->state = $state;
        if ($state === self::ROWS_FETCHED) {
            $this->rowFetched(null);
        }
        if (empty($this->deferreds[$state])) {
            return;
        }
        foreach ($this->deferreds[$state] as [$deferred, $rows, $cb]) {
            \assert($deferred instanceof DeferredFuture);
            $deferred->complete($cb ? $cb($rows) : $rows);
        }
        $this->deferreds[$state] = [];
    }

    public function rowFetched(?array $row): void
    {
        if ($row !== null) {
            $this->rows[$this->fetchedRows++] = $row;
        }
        [$entry, , $cb] = \current($this->deferreds[self::UNFETCHED]);
        if ($entry !== null) {
            unset($this->deferreds[self::UNFETCHED][\key($this->deferreds[self::UNFETCHED])]);
            \assert($entry instanceof DeferredFuture);
            $entry->complete($cb && $row ? $cb($row) : $row);
        }
    }

    public function error(\Throwable $e): void
    {
        foreach ($this->deferreds as $state) {
            foreach ($this->deferreds[$state] as [$deferred]) {
                \assert($deferred instanceof DeferredFuture);
                $deferred->error($e);
            }
            $this->deferreds[$state] = [];
        }
    }

    /**
     * @codeCoverageIgnore
     */
    public function __debugInfo(): array
    {
        $tmp = clone $this;
        foreach ($tmp->deferreds as &$type) {
            foreach ($type as &$entry) {
                unset($entry[0], $entry[2]);
            }
        }

        return (array) $tmp;
    }
}
