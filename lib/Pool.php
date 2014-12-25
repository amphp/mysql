<?php

namespace Mysql;

class Pool {
	private $reactor;
	private $connector;
	private $connections = [];
	private $connectionMap = [];
	private $ready = [];
	private $readyMap = [];
	private $connectionFuture;
	private $virtualConnection;
	private $config = true;
	private $limit;

	public function __construct($connStr, $sslOptions = null, \Amp\Reactor $reactor = null) {
		$this->reactor = $reactor ?: \Amp\getReactor();
		$this->connector = new \Nbsock\Connector($this->reactor);

		$db = null;
		$limit = INF;

		// well, yes. I *had* to document that behavior change. Future me, feel free to kill me ;-)
		foreach (explode(";", $connStr) as $param) {
			if (PHP_VERSION_ID < 70000) {
				list($$key, $key) = array_reverse(explode("=", $param, 2));
			} else {
				list($key, $$key) = explode("=", $param, 2);
			}
		}
		if (!isset($host, $user, $pass)) {
			throw new \Exception("Required parameters host, user and pass need to be passed in connection string");
		}

		$this->config = new ConnectionConfig;
		$this->resolveHost($host);
		$this->config->user = $user;
		$this->config->pass = $pass;
		$this->config->db = $db;

		if (is_array($sslOptions)) {
			$this->config->ssl = $sslOptions;
		} else {
			$this->config->ssl = $sslOptions ? [] : null;
		}

		$this->limit = $limit;
		$this->initLocal();
		$this->addConnection();
	}

	private function initLocal() {
		$this->virtualConnection = new VirtualConnection;
		$this->config->ready = function($conn) { $this->ready($conn); };
		$this->config->restore = function() { return $this->getReadyConnection(); };
		$this->config->busy = function($conn) { unset($this->ready[$this->readyMap[spl_object_hash($conn)]]); };
	}

	/** First parameter may be collation too, then charset is determined by the prefix of collation */
	public function setCharset($charset, $collate = "") {
		if ($collate === "" && false !== $off = strpos($charset, "_")) {
			$collate = $charset;
			$charset = substr($collate, 0, $off);
		}

		$this->ready = [];
		$this->config->charset = $charset;
		$this->config->collate = $collate;
		foreach ($this->connections as $conn) {
			if ($conn->alive()) {
				$conn->setCharset($charset, $collate);
			}
		}
	}

	public function useExceptions($set) {
		$this->config->exceptions = $set;
	}

	private function resolveHost($host) {
		$index = strpos($host, ':');

		if($index === false) {
			$this->config->host = $host;
			$this->config->resolvedHost = "tcp://$host:3306";
		} else if($index === 0) {
			$this->config->host = "localhost";
			$this->config->resolvedHost = "tcp://localhost:" . (int) substr($host, 1);
		} else {
			list($host, $port) = explode(':', $host, 2);
			$this->config->host = $host;
			$this->config->resolvedHost = "tcp://$host:" . (int) $port;
		}
	}

	private function addConnection() {
		if (count($this->connections) >= $this->limit) {
			return;
		}

		$this->connections[] = $conn = new Connection($this->reactor, $this->config);
		end($this->connections);
		$this->connectionMap[spl_object_hash($conn)] = key($this->connections);
		$this->connectionFuture = $conn->connect($this->connector);
		$this->connectionFuture->when(function($error) use ($conn) {
			if ($error) {
				return;
			}

			if ($this->config->charset != "utf8mb4" || ($this->config->collate != "" && $this->config->collate != "utf8mb4_general_ci")) {
				$conn->setCharset($this->config->charset, $this->config->collate);
			}
		});
	}

	private function ready($conn) {
		if (list($method, $args) = $call = $this->virtualConnection->getCall()) {
			call_user_func_array([$conn, $method], $args);
		} else {
			$this->ready[] = $conn;
			end($this->ready);
			$this->readyMap[spl_object_hash($conn)] = key($this->ready);
			reset($this->ready);
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

		while (list($key, $conn) = each($this->ready)) {
			unset($this->ready[$key]);
			if ($conn->alive()) {
				return $conn;
			}
		}

		$this->addConnection();

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

	// @TODO really use this?
	public function getConnection() {
		$pool = clone $this;
		$pool->limit = 0;
		$pool->config = clone $pool->config;
		$pool->initLocal();
		return $this->getReadyConnection()->getThis()->when(function($error, $conn) use ($pool) {
			$hash = spl_object_hash($conn);
			unset($this->connections[$this->connectionMap[$hash]], $this->connectionMap[$hash]);
			$pool->limit = 1;
			$pool->connections = [$conn];
			$pool->connectionMap[$hash] = 0;
			$pool->ready($conn);
		});
	}

	public function __destruct() {
		$this->close();
	}

	public function close() {
		foreach ($this->connections as $conn) {
			$conn->close();
		}
	}
}