<?php

namespace Amp\Mysql;

use Amp\Future;
use Amp\Success;

class ResultSet {
	private $connInfo;
	private $result;

	public function __construct(ConnectionState $state, ResultProxy $result) {
		$this->connInfo = $state;
		$this->result = $result;
	}
	
	public function getFields() {
		if ($this->result->state >= ResultProxy::COLUMNS_FETCHED) {
			return new Success($this->result->columns);
		} else {
			$future = new Future;
			$this->result->futures[ResultProxy::COLUMNS_FETCHED][] = [$future, &$this->result->columns];
			return $future;
		}
	}

	public function rowCount() {
		if ($this->result->state == ResultProxy::ROWS_FETCHED) {
			return new Success(count($this->result->rows));
		} else {
			$future = new Future;
			$this->result->futures[ResultProxy::ROWS_FETCHED][] = [$future, null, function () { return count($this->result->rows); }];
			return $future;
		}
	}

	protected function genericFetchAll($cb) {
		if ($this->result->state == ResultProxy::ROWS_FETCHED) {
			return new Success($cb($this->result->rows));
		} else {
			$future = new Future;
			$this->result->futures[ResultProxy::ROWS_FETCHED][] = [$future, &$this->result->rows, $cb];
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
			$names = array_column($this->result->columns, "name");
			return array_map(function($row) use ($names) {
				return (object) array_combine($names, $row);
			}, $rows ?: []);
		});
	}

	public function fetchAll() {
		return $this->genericFetchAll(function($rows) {
			$names = array_column($this->result->columns, "name");
			return array_map(function($row) use ($names) {
				return array_combine($names, $row) + $row;
			}, $rows ?: []);
		});
	}

	protected function genericFetch(callable $cb = null) {
		if ($this->result->userFetched < $this->result->fetchedRows) {
			$row = $this->result->rows[$this->result->userFetched++];
			return new Success($cb ? $cb($row) : $row);
		} elseif ($this->result->state == ResultProxy::ROWS_FETCHED) {
			return new Success(null);
		} else {
			$future = new Future;
			$this->result->futures[ResultProxy::SINGLE_ROW_FETCH][] = [$future, null, $cb];
			return $future;
		}
	}

	public function fetchRow() {
		return $this->genericFetch();
	}

	public function fetchObject() {
		return $this->genericFetch(function ($row) {
			return (object) array_combine(array_column($this->result->columns, "name"), $row);
		});
	}

	public function fetch() {
		return $this->genericFetch(function ($row) {
			return array_combine(array_column($this->result->columns, "name"), $row) + $row;
		});
	}

	public function getConnInfo() {
		return clone $this->connInfo;
	}

	public function next() {
		return $this->result->next ?: $this->result->next = new Future;
	}

}
