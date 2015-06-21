<?php

namespace Amp\Mysql;

use Amp\Deferred;
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
			$deferred = new Deferred;
			$this->result->deferreds[ResultProxy::COLUMNS_FETCHED][] = [$deferred, &$this->result->columns, null];
			return $deferred->promise();
		}
	}

	public function rowCount() {
		if ($this->result->state == ResultProxy::ROWS_FETCHED) {
			return new Success(count($this->result->rows));
		} else {
			$deferred = new Deferred;
			$this->result->deferreds[ResultProxy::ROWS_FETCHED][] = [$deferred, null, function () { return count($this->result->rows); }];
			return $deferred->promise();
		}
	}

	protected function genericFetchAll($cb) {
		if ($this->result->state == ResultProxy::ROWS_FETCHED) {
			return new Success($cb($this->result->rows));
		} else {
			$deferred = new Deferred;
			$this->result->deferreds[ResultProxy::ROWS_FETCHED][] = [$deferred, &$this->result->rows, $cb];
			return $deferred->promise();
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
			$deferred = new Deferred;
			$this->result->deferreds[ResultProxy::SINGLE_ROW_FETCH][] = [$deferred, null, $cb];
			return $deferred->promise();
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
		$deferred = $this->result->next ?: $this->result->next = new Deferred;
		return $deferred->promise();
	}

}
