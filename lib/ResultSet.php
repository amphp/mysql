<?php

namespace Mysql;

use Amp\Future;
use Amp\Success;

class ResultSet {
	private $columnCount;
	private $columns;
	private $reactor;
	private $columnsToFetch;
	private $rows;
	private $fetchedRows = 0;
	private $userFetched = 0;
	private $futures = [self::SINGLE_ROW_FETCH => [], self::COLUMNS_FETCHED => [], self::ROWS_FETCHED => []];
	private $state = self::UNFETCHED;
	private $next;

	const UNFETCHED = 0;
	const COLUMNS_FETCHED = 1;
	const ROWS_FETCHED = 2;

	const SINGLE_ROW_FETCH = 255;

	public function __construct(\Amp\Reactor $reactor) {
		$this->reactor = $reactor;
	}

	private function setColumns($columns) {
		$this->columnCount = $this->columnsToFetch = $columns;
	}

	public function getFields() {
		if ($this->state == self::COLUMNS_FETCHED) {
			return new Success($this->rows);
		} else {
			$future = new Future($this->reactor);
			$this->futures[self::COLUMNS_FETCHED][] = [$future, &$this->columns];
			return $future;
		}
	}

	public function fetchAll() {
		if ($this->state == self::ROWS_FETCHED) {
			return new Success($this->rows);
		} else {
			$future = new Future($this->reactor);
			$this->futures[self::ROWS_FETCHED][] = [$future, &$this->rows];
			return $future;
		}
	}

	public function fetchRow() {
		if ($this->userFetched < $this->fetchedRows) {
			return new Success($this->rows[$this->userFetched++]);
		} elseif ($this->state == self::ROWS_FETCHED) {
			return new Success(null);
		} else {
			return $this->futures[self::SINGLE_ROW_FETCH][] = new Future($this->reactor);
		}
	}

	private function updateState($state) {
		$this->state = $state;
		if (empty($this->futures[$state])) {
			return;
		}
		foreach ($this->futures[$state] as $entry) {
			$entry[0]->succeed($entry[1]);
		}
	}

	private function rowFetched($row) {
		$this->rows[$this->fetchedRows++] = $row;
		list($key, $entry) = each($this->futures[self::SINGLE_ROW_FETCH]);
		if ($key !== null) {
			unset($this->futures[self::SINGLE_ROW_FETCH][$key]);
			$entry->succeed($row);
		}
	}

	public function __debugInfo() {
		$tmp = clone $this;
		unset($tmp->reactor, $tmp->next);
		foreach ($tmp->futures as &$type) {
			foreach ($type as &$entry) {
				if (is_array($entry)) {
					$entry[0] = null;
				} else {
					$entry = null;
				}
			}
		}

		return $tmp;
	}

	public function next () {
		return $this->next ?: $this->next = new Future($this->reactor);
	}
}