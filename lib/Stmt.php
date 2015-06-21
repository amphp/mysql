<?php

namespace Amp\Mysql;

use Amp\Deferred;
use Amp\Success;

class Stmt {
	private $paramCount;
	private $numParamCount;
	private $named = [];
	private $byNamed;
	private $query;
	private $stmtId;
	private $prebound = [];
	private $conn; // when doing something on $conn, it must be checked if still same connection, else throw Exception! @TODO {or redo query, fetch???}
	private $virtualConn;

	private $result;

	public function __construct(Connection $conn, $query, $stmtId, $named, ResultProxy $result) {
		$this->conn = $conn;
		$this->query = $query;
		$this->stmtId = $stmtId;
		$this->result = $result;
		$this->numParamCount = $this->paramCount = $this->result->columnsToFetch;
		$this->byNamed = $named;
		foreach ($named as $name => $ids) {
			foreach ($ids as $id) {
				$this->named[$id] = $name;
				$this->numParamCount--;
			}
		}
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
					$this->stmtId = $stmt->stmtId;
					foreach ($this->prebound as $paramId => $msg) {
						$this->conn->bindParam($this->stmtId, $paramId, $msg);
					}
					while (list($deferred, $method, $args) = $this->virtualConn->getCall()) {
						if (isset($args[0])) {
							$args[0] = $this->stmtId;
						}
						if ($method == "execute") {
							$args[1] = &$this->result->params;
						}
						call_user_func_array([$this->conn(), $method], $args)->when(function($error, $result) use ($deferred) {
							if ($error) {
								$deferred->fail($error);
							} else {
								$deferred->succeed($result);
							}
						});
					}
				}
			});
			return $this->virtualConn = new VirtualConnection;
		}
		throw new \Exception("Connection went away, no way provided to restore connection via callable in ConnectionConfig::ready");
	}

	public function bind($paramId, $data) {
		if (is_numeric($paramId)) {
			if ($paramId >= $this->numParamCount) {
				throw new \Exception("Parameter id $paramId is not defined for this prepared statement");
			}
			$i = $paramId;
		} else {
			if (!isset($this->byNamed[$paramId])) {
				throw new \Exception("Parameter :$paramId is not defined for this prepared statement");
			}
			$array = $this->byNamed[$paramId];
			$i = reset($array);
		}

		do {
			$realId = -1;
			while (isset($this->named[++$realId]) || $i-- > 0) {
				if (!is_numeric($paramId) && $this->named[$realId] == $paramId) {
					break;
				}
			}

			$this->conn()->bindParam($this->stmtId, $realId, $data);
		} while (isset($array) && $i = next($array));

		if (isset($this->prebound[$paramId])) {
			$this->prebound[$paramId] .= $data;
		} else {
			$this->prebound[$paramId] = $data;
		}
	}

	public function execute($data = []) {
		if (count($data + $this->prebound) != $this->paramCount + count($this->named) - count($this->named, COUNT_RECURSIVE)) {
			throw new \Exception("Required arguments for executing prepared statement mismatch");
		}

		$prebound = $args = [];
		for ($unnamed = $i = 0; $i < $this->paramCount; $i++) {
			if (isset($this->named[$i])) {
				if (array_key_exists($this->named[$i], $data)) {
					$args[$i] = $data[$this->named[$i]];
				} elseif (!isset($this->prebound[$this->named[$i]])) {
					throw new \Exception("Named parameter {$this->named[$i]} missing for executing prepared statement");
				} else {
					$prebound[$i] = $this->prebound[$this->named[$i]];
				}
			} elseif (array_key_exists($unnamed, $data)) {
				$args[$i] = $data[$unnamed];
				$unnamed++;
			} elseif (!isset($this->prebound[$unnamed])) {
				throw new \Exception("Parameter $unnamed for prepared statement missing");
			} else {
				$prebound[$i] = $this->prebound[$unnamed++];
			}
		}

		return $this->conn()->execute($this->stmtId, $this->query, $this->result->params, $prebound, $args);
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
		if ($this->result->state >= ResultProxy::COLUMNS_FETCHED) {
			return new Success($this->result->columns);
		} elseif (isset($this->result->deferreds[ResultProxy::COLUMNS_FETCHED][0])) {
			return $this->result->deferreds[ResultProxy::COLUMNS_FETCHED][0][0]->promise();
		} else {
			$deferred = new Deferred;
			$this->result->deferreds[ResultProxy::COLUMNS_FETCHED][0] = [$deferred, &$this->result->columns, null];
			return $deferred->promise();
		}
	}

	public function connInfo() {
		return $this->conn->getConnInfo();
	}

	public function __destruct() {
		$this->close();
	}

	public function __debugInfo() {
		$tmp = clone $this;
		unset($tmp->conn);

		return (array) $tmp;
	}
}