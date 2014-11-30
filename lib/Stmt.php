<?php

namespace Mysql;

use Amp\Future;
use Amp\Success;

class Stmt {
	private $columnCount;
	private $paramCount;
	private $columns = [];
	private $params = [];
	private $stmtId;
	private $reactor;
	private $columnsToFetch;
	private $prebound = [];
	private $futures = [];
	private $conn; // when doing something on $conn, it must be checked if still same connection, else throw Exception! @TODO {or redo query, fetch???}

	private $state = ResultSet::UNFETCHED;

	public function __construct(\Amp\Reactor $reactor, Connection $conn, $stmtId, $columns, $params) {
		$this->reactor = $reactor;
		$this->conn = $conn;
		$this->stmtId = $stmtId;
		$this->columnCount = $columns;
		$this->paramCount = $this->columnsToFetch = $params;
	}

	public function bind($paramId, $data) {
		// @TODO validate $paramId
		$this->prebound[$paramId] = true;
		$this->conn->bindParam($this->stmtId, $paramId, $data);
	}

	public function execute($data = []) {
		if (count($data + $this->prebound) != count($this->params)) {
			throw new \Exception("Required arguments for executing prepared statement mismatch");
		}
		return $this->conn->execute($this->stmtId, $this->params, $this->prebound, $data);
	}

	public function close() {
		if (isset($this->conn) && $this->conn instanceof Connection) { // might be already dtored
			$this->conn->closeStmt($this->stmtId);
		}
	}

	public function reset() {
		$this->conn->resetStmt($this->stmtId);
	}

	// @TODO not necessary, see cursor?!
	public function fetch() {
		return $this->conn->fetchStmt($this->stmtId);
	}

	public function getFields() {
		if ($this->state >= ResultSet::COLUMNS_FETCHED) {
			return new Success($this->columns);
		} else {
			return $this->futures[] = new Future($this->reactor);
		}
	}

	private function updateState() {
		foreach ($this->futures as $future) {
			$future->succeed($this->columns);
		}
		$this->futures = [];
		$this->state = ResultSet::COLUMNS_FETCHED;
	}

	public function connInfo() {
		return $this->conn->getConnInfo();
	}

	public function __destruct() {
		$this->close();
	}

	public function __debugInfo() {
		$tmp = clone $this;
		unset($tmp->reactor, $tmp->conn);
		foreach ($tmp->futures as &$future) {
			$future = null;
		}

		return $tmp;
	}
}