<?php

namespace Amp\Mysql;

use Amp\Deferred;
use Amp\Success;

class Connection {
	const REFRESH_GRANT = 0x01;
	const REFRESH_LOG = 0x02;
	const REFRESH_TABLES = 0x04;
	const REFRESH_HOSTS = 0x08;
	const REFRESH_STATUS = 0x10;
	const REFRESH_THREADS = 0x20;
	const REFRESH_SLAVE = 0x40;
	const REFRESH_MASTER = 0x80;

	protected $processor;

	public function __construct($config, $sslOptions = null) {
		if (!$config instanceof ConnectionConfig) {
			$config = self::parseConnStr($config, $sslOptions);
		}
		if ($config->resolvedHost === null) {
			$this->resolveHost($config);
		}
		$hash = spl_object_hash($this);
		$ready = static function() use ($hash, $config) {
			$cb = $config->ready;
			if (isset($cb)) {
				$cb($hash);
			}
		};
		$busy = static function() use ($hash, $config) {
			$cb = $config->busy;
			if (isset($cb)) {
				$cb($hash);
			}
		};
		$restore = static function($init) use ($hash, $config) {
			$cb = $config->restore;
			if (isset($cb)) {
				return $cb($hash, $init);
			}
		};
		$this->processor = new Processor($ready, $busy, $restore);
		$this->processor->config = $config;
	}
	
	

	public static function parseConnStr($connStr, $sslOptions = null) {
		$db = null;

		// well, yes. I *had* to document that behavior change. Future me, feel free to kill me ;-)
		foreach (explode(";", $connStr) as $param) {
			if (PHP_VERSION_ID < 70000) {
				list($$key, $key) = array_reverse(array_map("trim", explode("=", $param, 2)));
			} else {
				list($key, $$key) = array_map("trim", explode("=", $param, 2));
			}
		}
		if (!isset($host, $user, $pass)) {
			throw new \Exception("Required parameters host, user and pass need to be passed in connection string");
		}

		$config = new ConnectionConfig;
		$config->host = $host;
		$config->user = $user;
		$config->pass = $pass;
		$config->db = $db;

		if (is_array($sslOptions)) {
			if (isset($sslOptions["key"])) {
				$config->key = $sslOptions["key"];
				unset($sslOptions["key"]);
			}
			$config->ssl = $sslOptions;
		} else {
			$config->ssl = $sslOptions ? [] : null;
		}

		return $config;
	}

	private function resolveHost($config) {
		$index = strpos($config->host, ':');

		if ($index === false) {
			$config->resolvedHost = "tcp://{$config->host}:3306";
		} else if ($index === 0) {
			$config->host = "localhost";
			$config->resolvedHost = "tcp://localhost:" . (int) substr($config->host, 1);
		} else {
			list($host, $port) = explode(':', $config->host, 2);
			$config->host = $host;
			$config->resolvedHost = "tcp://$host:" . (int) $port;
		}
	}

	public function useExceptions($set) {
		$this->processor->config->exceptions = $set;
	}

	public function alive() {
		return $this->processor->alive();
	}

	public function isReady() {
		return $this->processor->isReady();
	}

	public function forceClose() {
		$this->processor->closeSocket();
	}

	public function getConfig() {
		return clone $this->processor->config;
	}

	/* Technical function to be used in combination with Pool */
	public function getThis() {
		return new Success($this);
	}

	public function connect() {
		return $this->processor->connect();
	}
	
	public function getConnInfo() {
		return $this->processor->getConnInfo();
	}

	public function setCharset($charset, $collate = "") {
		if ($collate === "" && false !== $off = strpos($charset, "_")) {
			$collate = $charset;
			$charset = substr($collate, 0, $off);
		}

		$this->processor->config->charset = $charset;
		$this->processor->config->collate = $collate;

		$query = "SET NAMES '$charset'".($collate == "" ? "" : " COLLATE '$collate'");
		return $this->query($query);
	}

	/** @see 14.6.2 COM_QUIT */
	public function close() {
		$processor = $this->processor;
		return $processor->startCommand(static function() use ($processor) {
			$processor->sendPacket("\x01");
			$processor->initClosing();
		})->onResolve(static function() use ($processor) {
			$processor->closeSocket();
		});
	}

	/** @see 14.6.3 COM_INIT_DB */
	public function useDb($db) {
		$processor = $this->processor;
		return $processor->startCommand(static function() use ($processor, $db) {
			$processor->config->db = $db;
			$processor->sendPacket("\x02$db");
		});
	}

	/** @see 14.6.4 COM_QUERY */
	public function query($query) {
		$processor = $this->processor;
		return $processor->startCommand(static function() use ($processor, $query) {
			$processor->setQuery($query);
			$processor->sendPacket("\x03$query");
		});
	}

	/** @see 14.6.5 COM_FIELD_LIST */
	public function listFields($table, $like = "%") {
		$processor = $this->processor;
		return $processor->startCommand(static function() use ($processor, $table, $like) {
			$processor->sendPacket("\x04$table\0$like");
			$processor->setFieldListing();
		});
	}

	public function listAllFields($table, $like = "%") {
		$deferred = new Deferred;

		$columns = [];
		$when = function($error, $array) use (&$columns, &$when, $deferred) {
			if ($error) {
				$deferred->fail($error);
				return;
			}
			if ($array === null) {
				$deferred->resolve($columns);
				return;
			}
			list($columns[], $promise) = $array;
			$promise->onResolve($when);
		};
		$this->listFields($table, $like)->onResolve($when);

		return $deferred->promise();
	}

	/** @see 14.6.6 COM_CREATE_DB */
	public function createDatabase($db) {
		$processor = $this->processor;
		return $processor->startCommand(static function() use ($processor, $db) {
			$processor->sendPacket("\x05$db");
		});
	}

	/** @see 14.6.7 COM_DROP_DB */
	public function dropDatabase($db) {
		$processor = $this->processor;
		return $processor->startCommand(static function() use ($processor, $db) {
			$processor->sendPacket("\x06$db");
		});
	}

	/**
	 * @param $subcommand int one of the self::REFRESH_* constants
	 * @see 14.6.8 COM_REFRESH
	 */
	public function refresh($subcommand) {
		$processor = $this->processor;
		return $processor->startCommand(static function() use ($processor, $subcommand) {
			$processor->sendPacket("\x07" . chr($subcommand));
		});
	}

	/** @see 14.6.9 COM_SHUTDOWN */
	public function shutdown() {
		$processor = $this->processor;
		return $processor->startCommand(static function() use ($processor) {
			$processor->sendPacket("\x08\x00"); /* SHUTDOWN_DEFAULT / SHUTDOWN_WAIT_ALL_BUFFERS, only one in use */
		});
	}

	/** @see 14.6.10 COM_STATISTICS */
	public function statistics() {
		$processor = $this->processor;
		return $processor->startCommand(static function() use ($processor) {
			$processor->sendPacket("\x09");
			$processor->setStatisticsReading();
		});
	}

	/** @see 14.6.11 COM_PROCESS_INFO */
	public function processInfo() {
		$processor = $this->processor;
		return $processor->startCommand(static function() use ($processor) {
			$processor->sendPacket("\x0a");
			$processor->setQuery("SHOW PROCESSLIST");
		});
	}

	/** @see 14.6.13 COM_PROCESS_KILL */
	public function killProcess($process) {
		$processor = $this->processor;
		return $processor->startCommand(static function() use ($processor, $process) {
			$processor->sendPacket("\x0c" . DataTypes::encode_int32($process));
		});
	}

	/** @see 14.6.14 COM_DEBUG */
	public function debugStdout() {
		$processor = $this->processor;
		return $processor->startCommand(static function() use ($processor) {
			$processor->sendPacket("\x0d");
		});
	}

	/** @see 14.6.15 COM_PING */
	public function ping() {
		$processor = $this->processor;
		return $processor->startCommand(static function() use ($processor) {
			$processor->sendPacket("\x0e");
		});
	}

	/** @see 14.6.18 COM_CHANGE_USER */
	/* @TODO broken, my test server doesn't support that command, can't test now
	public function changeUser($user, $pass, $db = null) {
		return $this->startCommand(function() use ($user, $pass, $db) {
			$this->config->user = $user;
			$this->config->pass = $pass;
			$this->config->db = $db;
			$payload = "\x11";

			$payload .= "$user\0";
			$auth = $this->secureAuth($this->config->pass, $this->authPluginData);
			if ($this->capabilities & self::CLIENT_SECURE_CONNECTION) {
				$payload .= ord($auth) . $auth;
			} else {
				$payload .= "$auth\0";
			}
			$payload .= "$db\0";

			$this->sendPacket($payload);
			$this->parseCallback = [$this, "authSwitchRequest"];
		});
	}
	*/

	/** @see 14.6.19 COM_RESET_CONNECTION */
	public function resetConnection() {
		$processor = $this->processor;
		return $processor->startCommand(static function() use ($processor) {
			$processor->sendPacket("\x1f");
		});
	}

	/** @see 14.7.4 COM_STMT_PREPARE */
	public function prepare($query, $data = null) {
		$processor = $this->processor;
		$promise = $processor->startCommand(static function() use ($processor, $query) {
			$processor->setPrepare($query);
			$regex = <<<'REGEX'
(["'`])(?:\\(?:\\|\1)|(?!\1).)*+\1(*SKIP)(*F)|(\?)|:([a-zA-Z_]+)
REGEX;

			$index = 0;
			$query = preg_replace_callback("~$regex~ms", function ($m) use ($processor, &$index) {
				if ($m[2] !== "?") {
					$processor->named[$m[3]][] = $index;
				}
				$index++;
				return "?";
			}, $query);
			$processor->sendPacket("\x16$query");
		});

		if ($data === null) {
			return $promise;
		}

		$retDeferred = new Deferred;
		$promise->onResolve(static function($error, $stmt) use ($retDeferred, $data) {
			if ($error) {
				$retDeferred->fail($error);
			} else {
				$retDeferred->resolve($stmt->execute($data));
			}
		});

		return $retDeferred->promise();
	}
	
	public function __destruct() {
		$this->processor->delRef();
	}
}