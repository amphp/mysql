<?php

namespace Mysql;

use Amp\Future;
use Nbsock\Connector;

/**
 * @TODO limit?
 */

class Pool {
	private $host;
	private $db;
	private $user;
	private $connector;
	private $connections = [];
	private $ready = [];
	private $connectionFuture;
	private $virtualConnection;

	public function __construct($host, $user, $pass, $db = null, \Amp\Reactor $reactor = null) {
		$this->reactor = $reactor ?: \Amp\reactor();
		$this->connector = new Connector($this->reactor);
		$this->host = $host;
		$this->resolvedHost = "tcp://$host:3306"; // @TODO allow full hosts with port and protocol...
		$this->user = $user;
		$this->pass = $pass;
		$this->db = $db;
		$this->virtualConnection = new VirtualConnection($this->reactor);
		$this->addConnection();
	}

	private function addConnection() {
		$this->connections[] = $conn = new Connection($this->reactor, $this->connector, function () use (&$conn) { $this->ready($conn); }, $this->host, $this->resolvedHost, $this->user, $this->pass, $this->db);
		$this->connectionFuture = $conn->connect();
	}

	private function ready($conn) {
		if (list($method, $args) = $call = $this->virtualConnection->getCall()) {
			call_user_func_array([$conn, $method], $args);
		} else {
			$this->ready[] = $conn;
		}
	}

	public function init() {
		return $this->connectionFuture;
	}

	/** @return Connection */
	protected function &getReadyConnection() {
		if (count($this->ready) < 2) {
			$this->addConnection();
		}

		if (list($key, $conn) = current($this->ready)) {
			unset($this->ready[$key]);
			return $conn;
		}

		return $this->virtualConnection;
	}

	public function query($query) {
		return $this->getReadyConnection()->query($query);
	}

	public function listFields($table, $like = "%") {
		return $this->getReadyConnection()->listFields($table, $like);
	}

	public function listAllFields($table, $like = "%") {
		return $this->getReadyConnection()->listAllFields($table, $like);
	}

	public function createDatabase($db) {
		return $this->getReadyConnection()->createDatabase($db);
	}

	public function dropDatabase($db) {
		return $this->getReadyConnection()->dropDatabase($db);
	}
}