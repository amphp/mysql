<?php

namespace Mysql;

class ResultProxy {
	public $columnCount;
	public $columns = [];
	public $columnsToFetch;
	public $rows = null;
	public $fetchedRows = 0;
	public $userFetched = 0;
	public $futures = [self::SINGLE_ROW_FETCH => [], self::COLUMNS_FETCHED => [], self::ROWS_FETCHED => []];
	public $state = self::UNFETCHED;
	public $next;

	const UNFETCHED = 0;
	const COLUMNS_FETCHED = 1;
	const ROWS_FETCHED = 2;

	const SINGLE_ROW_FETCH = 255;

	public function setColumns($columns) {
		$this->columnCount = $this->columnsToFetch = $columns;
	}

	public function updateState($state) {
		$this->state = $state;
		if ($state == ResultProxy::ROWS_FETCHED) {
			$this->rowFetched(null);
		}
		if (empty($this->futures[$state])) {
			return;
		}
		foreach ($this->futures[$state] as list($future, $rows, $cb)) {
			$future->succeed($cb ? $cb($rows) : $rows);
		}
		$this->futures[$state] = [];
	}

	public function rowFetched($row) {
		if ($row !== null) {
			$this->rows[$this->fetchedRows++] = $row;
		}
		list($key, list($entry, , $cb)) = each($this->futures[ResultProxy::SINGLE_ROW_FETCH]);
		if ($key !== null) {
			unset($this->futures[ResultProxy::SINGLE_ROW_FETCH][$key]);
			$entry->succeed($cb && $row ? $cb($row) : $row);
		}
	}

	public function __debugInfo() {
		$tmp = clone $this;
		foreach ($tmp->futures as &$type) {
			foreach ($type as &$entry) {
				$entry[2] = null;
			}
		}

		return (array) $tmp;
	}

}