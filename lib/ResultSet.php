<?php

namespace Mysql;

use Amp\Future;
use Amp\Success;

class ResultSet {
	private $columnCount;
	private $columns = [];
	private $columnsToFetch;
	private $rows = null;
	private $fetchedRows = 0;
	private $userFetched = 0;
	private $futures = [self::SINGLE_ROW_FETCH => [], self::COLUMNS_FETCHED => [], self::ROWS_FETCHED => []];
	private $state = self::UNFETCHED;
	private $next;
	private $connInfo;

	const UNFETCHED = 0;
	const COLUMNS_FETCHED = 1;
	const ROWS_FETCHED = 2;

	const SINGLE_ROW_FETCH = 255;

	public function __construct(ConnectionState $state) {
		$this->connInfo = $state;
	}

	private function setColumns($columns) {
		$this->columnCount = $this->columnsToFetch = $columns;
	}

	public function getFields() {
		if ($this->state >= self::COLUMNS_FETCHED) {
			return new Success($this->columns);
		} else {
			$future = new Future;
			$this->futures[self::COLUMNS_FETCHED][] = [$future, &$this->columns];
			return $future;
		}
	}

	public function rowCount() {
		if ($this->state == self::ROWS_FETCHED) {
			return new Success(count($this->rows));
		} else {
			$future = new Future;
			$this->futures[self::ROWS_FETCHED][] = [$future, null, function () { return count($this->rows); }];
			return $future;
		}
	}

	private function genericFetchAll($cb) {
		if ($this->state == self::ROWS_FETCHED) {
			return new Success($cb($this->rows));
		} else {
			$future = new Future;
			$this->futures[self::ROWS_FETCHED][] = [$future, &$this->rows, $cb];
			return $future;
		}
	}

	public function fetchRows() {
		return $this->genericFetchAll(function($rows) {
			return $rows ?: [];
		});
	}

	public function fetchObjects() {
		return $this->genericFetchAll(function($rows) {
			$names = array_column($this->columns, "name");
			return array_map(function($row) use ($names) {
				return (object) array_combine($names, $row);
			}, $rows ?: []);
		});
	}

	public function fetchAll() {
		return $this->genericFetchAll(function($rows) {
			$names = array_column($this->columns, "name");
			return array_map(function($row) use ($names) {
				return array_combine($names, $row) + $row;
			}, $rows ?: []);
		});
	}

	public function genericFetch($cb) {
		if ($this->userFetched < $this->fetchedRows) {
			return new Success($cb($this->rows[$this->userFetched++]));
		} elseif ($this->state == self::ROWS_FETCHED) {
			return new Success(null);
		} else {
			$future = new Future;
			$this->futures[self::SINGLE_ROW_FETCH][] = [$future, null, $cb];
			return $future;
		}
	}

	public function fetchRow() {
		return $this->genericFetch(function ($row) {
			return $row;
		});
	}

	public function fetchObject() {
		return $this->genericFetch(function ($row) {
			return (object) array_combine(array_column($this->columns, "name"), $row);
		});
	}

	public function fetch() {
		return $this->genericFetch(function ($row) {
			return array_combine(array_column($this->columns, "name"), $row) + $row;
		});
	}

	private function updateState($state) {
		$this->state = $state;
		if ($state == self::ROWS_FETCHED) {
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

	private function rowFetched($row) {
		if ($row !== null) {
			$this->rows[$this->fetchedRows++] = $row;
		}
		list($key, list($entry, , $cb)) = each($this->futures[self::SINGLE_ROW_FETCH]);
		if ($key !== null) {
			unset($this->futures[self::SINGLE_ROW_FETCH][$key]);
			$entry->succeed($cb($row));
		}
	}

	public function getConnInfo() {
		return clone $this->connInfo;
	}

	public function __debugInfo() {
		$tmp = clone $this;
		unset($tmp->next);
		foreach ($tmp->futures as &$type) {
			foreach ($type as &$entry) {
				if (is_array($entry)) {
					$entry[0] = null;
				} else {
					$entry = null;
				}
			}
		}

		return (array) $tmp;
	}

	public function next() {
		return $this->next ?: $this->next = new Future;
	}

}
