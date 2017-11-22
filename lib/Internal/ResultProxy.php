<?php

namespace Amp\Mysql\Internal;

use Amp\Struct;

class ResultProxy {
    use Struct;

    public $columnCount;
    public $columns = [];
    public $params = [];
    public $columnsToFetch;
    public $rows = [];
    public $fetchedRows = 0;
    public $userFetched = 0;

    /** @var \Amp\Deferred[][] */
    public $deferreds = [self::SINGLE_ROW_FETCH => [], self::COLUMNS_FETCHED => [], self::ROWS_FETCHED => []];

    /** @var int */
    public $state = self::UNFETCHED;

    /** @var \Amp\Deferred|null */
    public $next;

    const UNFETCHED = 0;
    const COLUMNS_FETCHED = 1;
    const ROWS_FETCHED = 2;

    const SINGLE_ROW_FETCH = 255;

    public function setColumns(int $columns) {
        $this->columnCount = $this->columnsToFetch = $columns;
    }

    public function updateState(int $state) {
        $this->state = $state;
        if ($state == ResultProxy::ROWS_FETCHED) {
            $this->rowFetched(null);
        }
        if (empty($this->deferreds[$state])) {
            return;
        }
        foreach ($this->deferreds[$state] as list($deferred, $rows, $cb)) {
            $deferred->resolve($cb ? $cb($rows) : $rows);
        }
        $this->deferreds[$state] = [];
    }

    public function rowFetched($row) {
        if ($row !== null) {
            $this->rows[$this->fetchedRows++] = $row;
        }
        list($entry, , $cb) = current($this->deferreds[ResultProxy::SINGLE_ROW_FETCH]);
        if ($entry !== null) {
            unset($this->deferreds[ResultProxy::SINGLE_ROW_FETCH][key($this->deferreds[ResultProxy::SINGLE_ROW_FETCH])]);
            $entry->resolve($cb && $row ? $cb($row) : $row);
        }
    }

    public function __debugInfo() {
        $tmp = clone $this;
        foreach ($tmp->deferreds as &$type) {
            foreach ($type as &$entry) {
                unset($entry[0], $entry[2]);
            }
        }

        return (array) $tmp;
    }
}
