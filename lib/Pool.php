<?php

namespace Mysql;

use Amp\Future;

class Pool {
	private $reactor;
	private $connector = null;
	private $connections = [];
	private $connectionMap = [];
	private $ready = [];
	private $readyMap = [];
	private $connectionFuture;
	private $virtualConnection;
	private $config;
	private $limit;

	public function __construct($connStr, $sslOptions = null, \Amp\Reactor $reactor = null) {
		$this->reactor = $reactor ?: \Amp\getReactor();

		if (preg_match("((?:^|;)\s*limit\s*=\s*([^;]*?)\s*(?:;|$))is", $connStr, $match, PREG_OFFSET_CAPTURE)) {
			$this->limit = (int) $match[1][0];
			$connStr = substr_replace($connStr, ";", $match[0][1], strlen($match[0][0]));
		} else {
			$this->limit = INF;
		}

		$this->config = Connection::parseConnStr($connStr, $sslOptions);

		$this->initLocal();
		$this->addConnection();
	}

	private function initLocal() {
		$this->virtualConnection = new VirtualConnection;
		$this->config->ready = function($conn) { $this->ready($conn); };
		/* @TODO ... pending queries ... */
		$this->config->restore = function($conn, $init) {
			$this->unmapConnection($conn);
			if ($init && empty($this->connections)) {
				$this->virtualConnection->fail(new \Exception("Connection failed"));
			}
			return $this->getReadyConnection();
		};
		$this->config->busy = function($conn) { if (isset($this->readyMap[$hash = spl_object_hash($conn)])) unset($this->ready[$this->readyMap[$hash]]); };
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
		foreach ($this->connections as $conn) {
			$conn->useExceptions($set);
		}
	}

	private function addConnection() {
		$this->reactor->immediately(function() {
			if (count($this->connections) >= $this->limit) {
				return;
			}

			$this->connections[] = $conn = new Connection($this->config, null, $this->reactor);
			end($this->connections);
			$this->connectionMap[spl_object_hash($conn)] = key($this->connections);
			$this->connectionFuture = $conn->connect($this->connector ?: $this->connector = new \Nbsock\Connector($this->reactor));
			$this->connectionFuture->when(function ($error) use ($conn) {
				if ($error) {
					$this->unmapConnection($conn);
					if (empty($this->connections)) {
						$this->virtualConnection->fail($error);
					}
					return;
				}

				if ($this->config->charset != "utf8mb4" || ($this->config->collate != "" && $this->config->collate != "utf8mb4_general_ci")) {
					$conn->setCharset($this->config->charset, $this->config->collate);
				}
			});
		});
	}

	private function ready($conn) {
		if (list($future, $method, $args) = $this->virtualConnection->getCall()) {
			$future->succeed(call_user_func_array([$conn, $method], $args));
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
		if ($this->limit < 0) {
			$this->limit *= -1;
		}
		if (count($this->ready) < 2) {
			$this->addConnection();
		}

		while (list($key, $conn) = each($this->ready)) {
			unset($this->ready[$key]);
			if ($conn->isReady()) {
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

	public function prepare($query, $data = null) {
		return $this->getReadyConnection()->prepare($query, $data);
	}

	/* extracts a connection and returns it, wrapped in a Promise */
	public function getConnection() {
		return $this->getReadyConnection()->getThis()->when(function($error, $conn) {
			$this->unmapConnection($conn);
		});
	}

	/* This method might be called multiple times with the same hash. Important is that it's unmapped immediately */
	private function unmapConnection($conn) {
		$hash = spl_object_hash($conn);
		if (isset($this->connectionMap[$hash])) {
			unset($this->connections[$this->connectionMap[$hash]], $this->connectionMap[$hash]);
		}
	}

	public function __destruct() {
		$this->close();
	}

	public function close() {
		foreach ($this->connections as $conn) {
			$conn->forceClose();
			$this->unmapConnection($conn);
		}
		$this->ready = [];
		$this->readyMap = [];
		$this->connector = null;
		$this->limit *= -1;
	}
}