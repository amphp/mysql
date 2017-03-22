<?php

namespace Amp\Mysql;

class Pool {
	private $connectionPool;

	public function __construct($connStr, $sslOptions = null) {
		if (preg_match("((?:^|;)\s*limit\s*=\s*([^;]*?)\s*(?:;|$))is", $connStr, $match, PREG_OFFSET_CAPTURE)) {
			$limit = (int) $match[1][0];
			$connStr = substr_replace($connStr, ";", $match[0][1], strlen($match[0][0]));
		} else {
			$limit = INF;
		}

		$config = Connection::parseConnStr($connStr, $sslOptions);
		$this->connectionPool = new ConnectionPool($config, $limit);
	}

	public function init() {
		return $this->connectionPool->getConnectionPromise();
	}

	public function setCharset($charset, $collate = "") {
		$this->connectionPool->setCharset($charset, $collate);
	}
	
	public function query($query) {
		return $this->connectionPool->getReadyConnection()->query($query);
	}

	public function listFields($table, $like = "%") {
		return $this->connectionPool->getReadyConnection()->listFields($table, $like);
	}

	public function listAllFields($table, $like = "%") {
		return $this->connectionPool->getReadyConnection()->listAllFields($table, $like);
	}

	public function createDatabase($db) {
		return $this->connectionPool->getReadyConnection()->createDatabase($db);
	}

	public function dropDatabase($db) {
		return $this->connectionPool->getReadyConnection()->dropDatabase($db);
	}

	public function refresh($subcommand) {
		return $this->connectionPool->getReadyConnection()->refresh($subcommand);
	}

	public function shutdown() {
		return $this->connectionPool->getReadyConnection()->shutdown();
	}

	public function statistics() {
		return $this->connectionPool->getReadyConnection()->statistics();
	}

	public function processInfo() {
		return $this->connectionPool->getReadyConnection()->processInfo();
	}

	public function killProcess($process) {
		return $this->connectionPool->getReadyConnection()->killProcess($process);
	}

	public function debugStdout() {
		return $this->connectionPool->getReadyConnection()->debugStdout();
	}

	public function ping() {
		return $this->connectionPool->getReadyConnection()->ping();
	}

	public function prepare($query, $data = null) {
		return $this->connectionPool->getReadyConnection()->prepare($query, $data);
	}

	/* extracts a Connection and returns it, wrapped in a Promise */
	public function getConnection() {
		return $this->connectionPool->extractConnection();
	}

	public function close() {
		$this->connectionPool->close();
	}

	public function __destruct() {
		$this->close();
	}
}
