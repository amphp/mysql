<?php

namespace Mysql;

use Amp\Future;
use Amp\Success;

class Stmt {
	private $columnCount;
	private $paramCount;
	private $columns = [];
	private $params = [];
	private $query;
	private $stmtId;
	private $reactor;
	private $columnsToFetch;
	private $prebound = [];
	private $futures = [];
	private $conn; // when doing something on $conn, it must be checked if still same connection, else throw Exception! @TODO {or redo query, fetch???}
	private $virtualConn;

	private $state = ResultSet::UNFETCHED;

	public function __construct(\Amp\Reactor $reactor, Connection $conn, $query, $stmtId, $columns, $params) {
		$this->reactor = $reactor;
		$this->conn = $conn;
		$this->query = $query;
		$this->stmtId = $stmtId;
		$this->columnCount = $columns;
		$this->paramCount = $this->columnsToFetch = $params;
	}

	private function conn() {
		if ($this->conn->alive()) {
			return $this->conn;
		}
		$restore = $this->conn->getConfig()->restore;
		if (isset($restore)) {
			$restore()->prepare($this->query)->when(function($error, $stmt) {
				if ($error) {
					while (list(, $args) = $this->virtualConn->getCall()) {
						end($args)->fail($error);
					}
				} else {
					$this->conn = $stmt->conn;
					$this->stmtId = $stmt->conn;
					foreach ($this->prebound as $paramId => $msg) {
						$this->conn->bindParam($this->stmtId, $paramId, $msg);
					}
					while (list($method, $args) = $this->virtualConn->getCall()) {
						$future = array_pop($args);
						call_user_func_array([$this->conn(), $method], $args)->when(function($error, $result) use ($future) {
							if ($error) {
								$future->fail($error);
							} else {
								$future->succeed($result);
							}
						});
					}
				}
			});
			return $this->virtualConn = new VirtualConnection($this->reactor);
		}
		throw new \Exception("Connection went away, no way provided to restore connection via callable in ConnectionConfig::ready");
	}

	public function bind($paramId, $data) {
		// @TODO validate $paramId
		$this->conn()->bindParam($this->stmtId, $paramId, $data);
		if (isset($this->prebound[$paramId])) {
			$this->prebound[$paramId] .= $data;
		} else {
			$this->prebound[$paramId] = $data;
		}
	}

	public function execute($data = []) {
		if (count($data + $this->prebound) != count($this->params)) {
			throw new \Exception("Required arguments for executing prepared statement mismatch");
		}
		return $this->conn()->execute($this->stmtId, $this->params, $this->prebound, $data);
	}

	public function close() {
		if (isset($this->conn) && $this->conn instanceof Connection) { // might be already dtored
			$this->conn->closeStmt($this->stmtId);
		}
	}

	public function reset() {
		$this->conn()->resetStmt($this->stmtId);
	}

	// @TODO not necessary, see cursor?!
	public function fetch() {
		return $this->conn()->fetchStmt($this->stmtId);
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