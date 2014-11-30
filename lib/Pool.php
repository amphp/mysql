<?php

namespace Mysql;

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
	private $config = true;

	public function __construct($host, $user, $pass, $db = null, \Amp\Reactor $reactor = null) {
		$this->reactor = $reactor ?: \Amp\reactor();
		$this->connector = new \Nbsock\Connector($this->reactor);
		$this->resolveHost($host);
		$this->user = $user;
		$this->pass = $pass;
		$this->db = $db;
		$this->virtualConnection = new VirtualConnection($this->reactor);
		$this->config = new ConnectionConfig;
		$this->config->ready = function ($conn) { $this->ready($conn); };
		$this->addConnection();
	}

	public function useExceptions($set) {
		$this->exceptions = $set;
	}

	private function resolveHost($host) {
		$index = strpos($host, ':');

		if($index === false) {
			$this->host = $host;
			$this->resolvedHost = "tcp://$host:3306";
		} else if($index === 0) {
			$this->host = "localhost";
			$this->resolvedHost = "tcp://localhost:" . (int) substr($host, 1);
		} else {
			list($host, $port) = explode(':', $host, 2);
			$this->host = $host;
			$this->resolvedHost = "tcp://$host:" . (int) $port;
		}
	}

	private function addConnection() {
		$this->connections[] = $conn = new Connection($this->reactor, $this->connector, $this->config, $this->host, $this->resolvedHost, $this->user, $this->pass, $this->db);
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

		if (list($key, $conn) = each($this->ready)) {
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

	public function refresh($subcommand) {
		return $this->getReadyConnection()->refresh($subcommand);
	}

	public function shutdown() {
		return $this->getReadyConnection()->shutdown();
	}

	public function statistics() {
		return $this->getReadyConnection()->statistics();
	}

	public function processInfo() {
		return $this->getReadyConnection()->processInfo();
	}

	public function killProcess($process) {
		return $this->getReadyConnection()->killProcess($process);
	}

	public function debugStdout() {
		return $this->getReadyConnection()->debugStdout();
	}

	public function ping() {
		return $this->getReadyConnection()->ping();
	}

	/* @TODO changeUser broken...
	public function changeUser($user, $pass, $db = null) {
		return $this->getReadyConnection()->changeUser($user, $pass, $db);
	}
	*/

	public function resetConnection() {
		return $this->getReadyConnection()->resetConnection();
	}

	public function prepare($query) {
		return $this->getReadyConnection()->prepare($query);
	}
}