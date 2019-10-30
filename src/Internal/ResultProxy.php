<?php

namespace Amp\Mysql\Internal;

use Amp\Sql\FailureException;
use Amp\Struct;

final class ResultProxy
{
    use Struct;

    public $columnCount;
    public $columns = [];
    public $params = [];
    public $columnsToFetch;
    public $rows = [];
    public $fetchedRows = 0;
    public $userFetched = 0;

    public $deferreds = [self::UNFETCHED => [], self::COLUMNS_FETCHED => [], self::ROWS_FETCHED => []];

    /** @var int */
    public $state = self::UNFETCHED;

    /** @var \Amp\Deferred|null */
    public $next;

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
            $deferred->resolve($cb ? $cb($rows) : $rows);
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
            $entry->resolve($cb && $row ? $cb($row) : $row);
        }
    }

    public function fail(FailureException $e): void
    {
        foreach ($this->deferreds as $state) {
            foreach ($this->deferreds[$state] as [$deferred]) {
                $deferred->fail($e);
            }
            $this->deferreds[$state] = [];
        }
    }

    /**
     * @return array
     *
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
