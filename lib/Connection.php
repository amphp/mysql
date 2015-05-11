<?php

namespace Mysql;
use Amp\Future;
use Amp\Reactor;
use Amp\Success;
use Nbsock\Connector;

/* @TODO
 * 14.2.3 Auth switch request??
 * 14.2.4 COM_CHANGE_USER
 */

class ParseState {
	const START = 0;
	const FETCH_PACKET = 1;
}

/** @see 14.1.3.4 Status Flags */
class StatusFlags {
	const SERVER_STATUS_IN_TRANS = 0x0001; // a transaction is active
	const SERVER_STATUS_AUTOCOMMIT = 0x0002; // auto-commit is enabled
	const SERVER_MORE_RESULTS_EXISTS = 0x0008;
	const SERVER_STATUS_NO_GOOD_INDEX_USED = 0x0010;
	const SERVER_STATUS_NO_INDEX_USED = 0x0020;
	const SERVER_STATUS_CURSOR_EXISTS = 0x0040; // Used by Binary Protocol Resultset to signal that COM_STMT_FETCH has to be used to fetch the row-data.
	const SERVER_STATUS_LAST_ROW_SENT = 0x0080;
	const SERVER_STATUS_DB_DROPPED = 0x0100;
	const SERVER_STATUS_NO_BACKSLASH_ESCAPES = 0x0200;
	const SERVER_STATUS_METADATA_CHANGED = 0x0400;
	const SERVER_QUERY_WAS_SLOW = 0x0800;
	const SERVER_PS_OUT_PARAMS = 0x1000;
	const SERVER_STATUS_IN_TRANS_READONLY = 0x2000; // in a read-only transaction
	const SERVER_SESSION_STATE_CHANGED = 0x4000; // connection state information has changed
}

/** @see 13.1.3.1.1 Session State Information */
class SessionStateTypes {
	const SESSION_TRACK_SYSTEM_VARIABLES = 0x00;
	const SESSION_TRACK_SCHEMA = 0x01;
	const SESSION_TRACK_STATE_CHANGE = 0x02;
}

class Connection {
	private $out = [];
	private $outBuf;
	private $outBuflen = 0;
	private $uncompressedOut = "";
	private $compressionBuf;
	private $compressionBuflen = 0;
	private $mysqlBuf;
	private $mysqlBuflen = 0;
	private $lastIn = true;
	private $packet;
	private $compressionSize;
	private $uncompressedSize;
	private $mysqlState = ParseState::START;
	private $compressionState = ParseState::START;
	private $protocol;
	private $seqId = -1;
	private $compressionId = -1;
	private $packetSize;
	private $packetType;
	private $socket;
	private $readGranularity = 8192;
	private $readWatcher = null;
	private $writeWatcher = null;
	private $watcherEnabled = false;
	private $authPluginDataLen;
	private $query;
	private $named = [];
	private $parseCallback = null;
	private $packetCallback = null;

	private $reactor;
	private $config;
	private $futures = [];
	private $onReady = [];
	private $result;
	private $oldDb = null;

	protected $connectionId;
	protected $authPluginData;
	protected $capabilities = 0;
	protected $serverCapabilities = 0;
	protected $authPluginName;
	protected $connInfo;

	protected $connectionState = self::UNCONNECTED;

	const MAX_PACKET_SIZE = 0xffffff;
	const MAX_UNCOMPRESSED_BUFLEN = 0xfffffb;

	const CLIENT_LONG_FLAG = 0x00000004;
	const CLIENT_CONNECT_WITH_DB = 0x00000008;
	const CLIENT_COMPRESS = 0x00000020;
	const CLIENT_PROTOCOL_41 = 0x00000200;
	const CLIENT_SSL = 0x00000800;
	const CLIENT_TRANSACTIONS = 0x00002000;
	const CLIENT_SECURE_CONNECTION = 0x00008000;
	const CLIENT_MULTI_STATEMENTS = 0x00010000;
	const CLIENT_MULTI_RESULTS = 0x00020000;
	const CLIENT_PS_MULTI_RESULTS = 0x00040000;
	const CLIENT_PLUGIN_AUTH = 0x00080000;
	const CLIENT_CONNECT_ATTRS = 0x00100000;
	const CLIENT_SESSION_TRACK = 0x00800000;
	const CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000;
	const CLIENT_DEPRECATE_EOF = 0x01000000;

	const OK_PACKET = 0x00;
	const EXTRA_AUTH_PACKET = 0x01;
	const LOCAL_INFILE_REQUEST = 0xfb;
	const EOF_PACKET = 0xfe;
	const ERR_PACKET = 0xff;

	const UNCONNECTED = 0;
	const ESTABLISHED = 1;
	const READY = 2;
	const CLOSING = 3;
	const CLOSED = 4;

	const REFRESH_GRANT = 0x01;
	const REFRESH_LOG = 0x02;
	const REFRESH_TABLES = 0x04;
	const REFRESH_HOSTS = 0x08;
	const REFREHS_STATUS = 0x10;
	const REFRESH_THREADS = 0x20;
	const REFRESH_SLAVE = 0x40;
	const REFRESH_MASTER = 0x80;

	public function __construct($config, $sslOptions = null, Reactor $reactor = null) {
		$this->reactor = $reactor ?: \Amp\getReactor();
		$this->connInfo = new ConnectionState;

		if (!$config instanceof ConnectionConfig) {
			$config = self::parseConnStr($config, $sslOptions);
		}
		if ($config->resolvedHost === null) {
			$this->resolveHost($config);
		}
		$this->config = $config;
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
		$this->config->exceptions = $set;
	}

	public function alive() {
		return $this->connectionState <= self::READY;
	}

	public function isReady() {
		return $this->connectionState === self::READY;
	}

	public function forceClose() {
		$this->closeSocket();
	}

	public function getConfig() {
		return clone $this->config;
	}

	/* Technical function to be used in combination with Pool */
	public function getThis() {
		return new Success($this);
	}

	private function ready() {
		if (empty($this->futures)) {
			if (empty($this->onReady)) {
				$cb = $this->config->ready;
				$this->out[] = null;
			} else {
				list($key, $cb) = each($this->onReady);
				unset($this->onReady[$key]);
			}
			if (isset($cb) && is_callable($cb)) {
				$cb($this);
			}
		}
	}

	public function connect(Connector $connector = null) {
		$connector = $connector ?: new \Nbsock\Connector($this->reactor);

		$future = new Future;
		$connector->connect($this->config->resolvedHost)->when(function ($error, $socket) use ($future) {
			if ($this->connectionState === self::CLOSED) {
				$future->succeed(null);
				if ($socket) {
					fclose($socket);
				}
				return;
			}

			if ($error) {
				$future->fail($error);
				if ($socket) {
					fclose($socket);
				}
				return;
			}

			$this->socket = $socket;
			$this->readWatcher = $this->reactor->onReadable($this->socket, [$this, "onInit"]);
			$this->futures[] = $future;
		});
		return $future;
	}

	public function onInit() {
		// reset internal state
		$this->compressionBuf = $this->mysqlBuf = "";
		$this->compressionBuflen = $this->mysqlBuflen = 0;
		$this->out = [];
		$this->seqId = $this->compressionId = -1;

		$this->reactor->cancel($this->readWatcher);
		$this->readWatcher = $this->reactor->onReadable($this->socket, [$this, "onRead"]);
		$this->onRead();
	}

	/** @return Future */
	private function getFuture() {
		list($key, $future) = each($this->futures);
		unset($this->futures[$key]);
		return $future;
	}

	private function appendTask($callback) {
		if (!empty($this->futures) || $this->connectionState != self::READY) {
			$this->onReady[] = $callback;
		} else {
			$cb = $this->config->busy;
			if (isset($cb)) {
				$cb($this);
			}
			$callback();
		}
	}

	public function getConnInfo() {
		return clone $this->connInfo;
	}

	private function startCommand($callback) {
		$future = new Future;
		$this->appendTask(function() use ($callback, $future) {
			$this->seqId = $this->compressionId = -1;
			$this->futures[] = $future;
			$callback();
		});
		return $future;
	}

	public function setCharset($charset, $collate = "") {
		if ($collate === "" && false !== $off = strpos($charset, "_")) {
			$collate = $charset;
			$charset = substr($collate, 0, $off);
		}

		$this->config->charset = $charset;
		$this->config->collate = $collate;

		$query = "SET NAMES '$charset'".($collate == "" ? "" : " COLLATE '$collate'");
		$this->appendTask(function() use ($query, &$future) {
			$future = $this->query($query);
		});
		return $future;
	}

	/** @see 14.6.2 COM_QUIT */
	public function close() {
		return $this->startCommand(function() {
			$this->sendPacket("\x01");
			$this->connectionState = self::CLOSING;
		})->when(function() {
			$this->closeSocket();
		});
	}

	/** @see 14.6.3 COM_INIT_DB */
	public function useDb($db) {
		return $this->startCommand(function() use ($db) {
			$this->oldDb = $this->config->db;
			$this->config->db = $db;
			$this->sendPacket("\x02$db");
		});
	}

	/** @see 14.6.4 COM_QUERY */
	public function query($query) {
		return $this->startCommand(function() use ($query) {
			$this->sendPacket("\x03$query");
			$this->query = $query;
			$this->packetCallback = [$this, "handleQuery"];
		});
	}

	/** @see 14.6.5 COM_FIELD_LIST */
	public function listFields($table, $like = "%") {
		return $this->startCommand(function() use ($table, $like) {
			$this->sendPacket("\x04$table\0$like");
			$this->parseCallback = [$this, "handleFieldlist"];
		});
	}

	public function listAllFields($table, $like = "%") {
		$future = new Future;

		$columns = [];
		$when = function($error, $array) use (&$columns, &$when, $future) {
			if ($error) {
				$future->fail($error);
				return;
			}
			if ($array === null) {
				$future->succeed($columns);
				return;
			}
			list($columns[], $promise) = $array;
			$promise->when($when);
		};
		$this->listFields($table, $like)->when($when);

		return $future;
	}

	/** @see 14.6.6 COM_CREATE_DB */
	public function createDatabase($db) {
		return $this->startCommand(function() use ($db) {
			$this->sendPacket("\x05$db");
		});
	}

	/** @see 14.6.7 COM_DROP_DB */
	public function dropDatabase($db) {
		return $this->startCommand(function() use ($db) {
			$this->sendPacket("\x06$db");
		});
	}

	/**
	 * @param $subcommand int one of the self::REFRESH_* constants
	 * @see 14.6.8 COM_REFRESH
	 */
	public function refresh($subcommand) {
		return $this->startCommand(function() use ($subcommand) {
			$this->sendPacket("\x07" . chr($subcommand));
		});
	}

	/** @see 14.6.9 COM_SHUTDOWN */
	public function shutdown() {
		return $this->startCommand(function() {
			$this->sendPacket("\x08\x00"); /* SHUTDOWN_DEFAULT / SHUTDOWN_WAIT_ALL_BUFFERS, only one in use */
		});
	}

	/** @see 14.6.10 COM_STATISTICS */
	public function statistics() {
		return $this->startCommand(function() {
			$this->sendPacket("\x09");
			$this->parseCallback = [$this, "readStatistics"];
		});
	}

	/** @see 14.6.11 COM_PROCESS_INFO */
	public function processInfo() {
		return $this->startCommand(function() {
			$this->sendPacket("\x0a");
			$this->packetCallback = [$this, "handleQuery"];
		});
	}

	/** @see 14.6.13 COM_PROCESS_KILL */
	public function killProcess($process) {
		return $this->startCommand(function() use ($process) {
			$this->sendPacket("\x0c" . DataTypes::encode_int32($process));
		});
	}

	/** @see 14.6.14 COM_DEBUG */
	public function debugStdout() {
		return $this->startCommand(function() {
			$this->sendPacket("\x0d");
		});
	}

	/** @see 14.6.15 COM_PING */
	public function ping() {
		return $this->startCommand(function() {
			$this->sendPacket("\x0e");
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
		return $this->startCommand(function() {
			$this->sendPacket("\x1f");
		});
	}

	/** @see 14.7.4 COM_STMT_PREPARE */
	public function prepare($query, $data = null) {
		$future = $this->startCommand(function() use ($query) {
			$this->query = $query;
			$regex = <<<'REGEX'
("|'|`)((?:\\\\|\\\1|(?!\1).)*+)\1|(\?)|:([a-zA-Z_]+)
REGEX;

			$index = 0;
			$query = preg_replace_callback("~$regex~ms", function ($m) use (&$index) {
				if (!isset($m[3])) {
					return $m[1] . $m[2] . $m[1];
				}
				if ($m[3] !== "?") {
					$this->named[$m[4]][] = $index;
				}
				$index++;
				return "?";
			}, $query);
			$this->sendPacket("\x16$query");
			$this->parseCallback = [$this, "handlePrepare"];
		});

		if ($data === null) {
			return $future;
		}

		$retFuture = new Future;
		$future->when(function($error, $stmt) use ($retFuture, $data) {
			if ($error) {
				$retFuture->fail($error);
			} else {
				try {
					$retFuture->succeed($stmt->execute($data));
				} catch (\Exception $e) {
					$retFuture->fail($e);
				}
			}
		});

		return $retFuture;
	}

	/** @see 14.7.5 COM_STMT_SEND_LONG_DATA */
	public function bindParam($stmtId, $paramId, $data) {
		$payload = "\x18";
		$payload .= DataTypes::encode_int32($stmtId);
		$payload .= DataTypes::encode_int16($paramId);
		$payload .= $data;
		$this->appendTask(function () use ($payload) {
			$this->out[] = null;
			$this->sendPacket($payload);
			$this->ready();
		});
	}

	/** @see 14.7.6 COM_STMT_EXECUTE */
	// prebound params: null-bit set, type MYSQL_TYPE_LONG_BLOB, no value
	// $params is by-ref, because the actual result object might not yet have been filled completely with data upon call of this method ...
	public function execute($stmtId, $query, &$params, $prebound, $data = []) {
		$future = new Future;
		$this->appendTask(function () use ($stmtId, $query, &$params, $prebound, $data, $future) {
			$payload = "\x17";
			$payload .= DataTypes::encode_int32($stmtId);
			$payload .= chr(0); // cursor flag // @TODO cursor types?!
			$payload .= DataTypes::encode_int32(1);
			$paramCount = count($params);
			$bound = !empty($data) || !empty($prebound);
			$types = "";
			$values = "";
			if ($paramCount) {
				$args = array_slice($data + array_fill(0, $paramCount, null), 0, $paramCount);
				$nullOff = strlen($payload);
				$payload .= str_repeat("\0", ($paramCount + 7) >> 3);
				foreach ($args as $paramId => $param) {
					if ($param === null) {
						$off = $nullOff + ($paramId >> 3);
						$payload[$off] = $payload[$off] | chr(1 << ($paramId % 8));
					} else {
						$bound = 1;
					}
					list($unsigned, $type, $value) = DataTypes::encodeBinary($param);
					if (isset($prebound[$paramId])) {
						$types .= chr(DataTypes::MYSQL_TYPE_LONG_BLOB);
					} else {
						$types .= chr($type);
					}
					$types .= $unsigned?"\x80":"\0";
					$values .= $value;
				}
				$payload .= chr($bound);
				if ($bound) {
					$payload .= $types;
					$payload .= $values;
				}
			}

			$this->query = $query;

			$this->out[] = null;
			$this->futures[] = $future;
			$this->sendPacket($payload);
			$this->packetCallback = [$this, "handleExecute"];
		});
		return $future; // do not use $this->startCommand(), that might unexpectedly reset the seqId!
	}

	/** @see 14.7.7 COM_STMT_CLOSE */
	public function closeStmt($stmtId) {
		$payload = "\x19" . DataTypes::encode_int32($stmtId);
		$this->appendTask(function () use ($payload) {
			if ($this->connectionState === self::READY) {
				$this->out[] = null;
				$this->sendPacket($payload);
			}
			$this->ready();
		});
	}

	/** @see 14.7.8 COM_STMT_RESET */
	public function resetStmt($stmtId) {
		$payload = "\x1a" . DataTypes::encode_int32($stmtId);
		$future = new Future;
		$this->appendTask(function () use ($payload, $future) {
			$this->out[] = null;
			$this->futures[] = $future;
			$this->sendPacket($payload);
		});
		return $future;
	}

	/** @see 14.8.4 COM_STMT_FETCH */
	public function fetchStmt($stmtId) {
		$payload = "\x1c" . DataTypes::encode_int32($stmtId) . DataTypes::encode_int32(1);
		$future = new Future;
		$this->appendTask(function () use ($payload, $future) {
			$this->out[] = null;
			$this->futures[] = $future;
			$this->sendPacket($payload);
			$this->ready();
		});
		return $future;
	}

	private function established() {
		// @TODO flags to use?
		$this->capabilities |= self::CLIENT_SESSION_TRACK | self::CLIENT_TRANSACTIONS | self::CLIENT_PROTOCOL_41 | self::CLIENT_SECURE_CONNECTION | self::CLIENT_MULTI_RESULTS | self::CLIENT_PS_MULTI_RESULTS | self::CLIENT_MULTI_STATEMENTS | self::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA;

		if (extension_loaded("zlib")) {
			$this->capabilities |= self::CLIENT_COMPRESS;
		}

		$this->writeWatcher = $this->reactor->onWritable($this->socket, [$this, "onWrite"], $enableNow = false);
	}

	/** @see 14.1.3.2 ERR-Packet */
	private function handleError() {
		$off = 1;

		err_packet: {
			$this->connInfo->errorCode = DataTypes::decode_int16(substr($this->packet, $off, 2));
			$off += 2;
			if ($this->capabilities & self::CLIENT_PROTOCOL_41) {
				// goto get_err_state;
			} else {
				goto fetch_err_msg;
			}
		}

		get_err_state: {
			$this->connInfo->errorState = substr($this->packet, $off, 6);

			$off += 6;
			// goto fetch_err_msg;
		}

		fetch_err_msg: {
			$this->connInfo->errorMsg = substr($this->packet, $off);

			// goto finished;
		}


		finished: {
			$this->parseCallback = null;
			if ($this->connectionState == self::READY) {
				// normal error
				if ($this->config->exceptions) {
					$this->getFuture()->fail(new QueryException("MySQL error ({$this->connInfo->errorCode}): {$this->connInfo->errorState} {$this->connInfo->errorMsg}", $this->query));
				} else {
					$this->getFuture()->succeed(false);
				}
				$this->query = null;
				$this->ready();
			} elseif ($this->connectionState < self::READY) {
				// connection failure
				$this->closeSocket();
				$this->getFuture()->fail(new InitializationException("Could not connect to {$this->config->resolvedHost}: {$this->connInfo->errorState} {$this->connInfo->errorMsg}"));
			}
		}
	}

	/** @see 14.1.3.1 OK-Packet */
	private function parseOk() {
		$off = 1;

		ok_packet: {
			$this->connInfo->affectedRows = DataTypes::decodeInt(substr($this->packet, $off), $intlen);
			$off += $intlen;
			// goto get_last_insert_id;
		}

		get_last_insert_id: {
			$this->connInfo->insertId = DataTypes::decodeInt(substr($this->packet, $off), $intlen);
			$off += $intlen;
			if ($this->capabilities & (self::CLIENT_PROTOCOL_41 | self::CLIENT_TRANSACTIONS)) {
				// goto get_status_flags;
			} else {
				goto fetch_status_info;
			}
		}

		get_status_flags: {
			$this->connInfo->statusFlags = DataTypes::decode_int16(substr($this->packet, $off));
			$off += 2;
			// goto get_warning_count;
		}

		get_warning_count: {
			$this->connInfo->warnings = DataTypes::decode_int16(substr($this->packet, $off));
			$off += 2;
			// goto fetch_status_info;
		}

		fetch_status_info: {
			if ($this->capabilities & self::CLIENT_SESSION_TRACK) {
				$this->connInfo->statusInfo = DataTypes::decodeString(substr($this->packet, $off), $intlen, $strlen);
				$off += $intlen + $strlen;
				if ($this->connInfo->statusFlags & StatusFlags::SERVER_SESSION_STATE_CHANGED) {
					goto fetch_state_changes;
				}
			} else {
				$this->connInfo->statusInfo = substr($this->packet, $off);
			}
			goto finished;
		}

		fetch_state_changes: {
			$sessionState = DataTypes::decodeString(substr($this->packet, $off), $intlen, $sessionStateLen);
			$len = 0;
			while ($len < $sessionStateLen) {
				$data = DataTypes::decodeString(substr($sessionState, $len + 1), $datalen);

				switch ($type = DataTypes::decode_int8(substr($sessionState, $len))) {
					case SessionStateTypes::SESSION_TRACK_SYSTEM_VARIABLES:
						$this->connInfo->sessionState[SessionStateTypes::SESSION_TRACK_SYSTEM_VARIABLES][DataTypes::decodeString($data, $intlen, $strlen)] = DataTypes::decodeString(substr($data, $intlen + $strlen));
						break;
					case SessionStateTypes::SESSION_TRACK_SCHEMA:
						$this->connInfo->sessionState[SessionStateTypes::SESSION_TRACK_SCHEMA] = DataTypes::decodeString($data);
						break;
					case SessionStateTypes::SESSION_TRACK_STATE_CHANGE:
						$this->connInfo->sessionState[SessionStateTypes::SESSION_TRACK_STATE_CHANGE] = DataTypes::decodeString($data);
						break;
					default:
						throw new \UnexpectedValueException("$type is not a valid mysql session state type");
				}

				$len += 1 + $datalen;
			}

			// goto finished;
		}

		finished: {
			return;
		}
	}

	private function handleOk() {
		var_dump(count($this->futures));
		$this->parseOk();
		$this->getFuture()->succeed($this->getConnInfo());
		$this->ready();
	}

	/** @see 14.1.3.3 EOF-Packet */
	private function parseEof() {
		$off = 1;

		eof_packet: {
			if ($this->capabilities & self::CLIENT_PROTOCOL_41) {
				$this->connInfo->warnings = DataTypes::decode_int16(substr($this->packet, $off));
				$off += 2;
				// goto get_eof_status_flags;
			} else {
				goto finished;
			}
		}

		get_eof_status_flags: {
			$this->connInfo->statusFlags = DataTypes::decode_int16(substr($this->packet, $off));
			// goto finished;
		}

		finished: {
			return;
		}
	}

	private function handleEof() {
		$this->parseEof();
		$this->getFuture()->succeed($this->getConnInfo());
		$this->ready();
	}

	/** @see 14.2.5 Connection Phase Packets */
	private function handleHandshake() {
		$off = 1;

		handshake_packet: {
			$this->protocol = $this->packetType;
			if ($this->protocol !== 0x0a) {
				throw new \UnexpectedValueException("Unsupported protocol version ".ord($this->packet)." (Expected: 10)");
			}
			// goto fetch_server_version;
		}

		fetch_server_version: {
			$this->connInfo->serverVersion = DataTypes::decodeNullString(substr($this->packet, $off), $len);
			$off += $len + 1;
			// goto get_connection_id;
		}

		get_connection_id: {
			$this->connectionId = DataTypes::decode_int32(substr($this->packet, $off));
			$off += 4;
			goto read_auth_plugin_data1;
		}

		read_auth_plugin_data1: {
			$this->authPluginData = substr($this->packet, $off, 8);
			$off += 8;
			// goto filler;
		}

		filler: {
			$off += 1;
			// goto read_capability_flags1;
		}

		read_capability_flags1: {
			$this->serverCapabilities = DataTypes::decode_int16(substr($this->packet, $off));
			$off += 2;
			if ($this->packetSize > $off) {
				// goto charset;
			} else {
				goto do_handshake;
			}
		}

		charset: {
			$this->connInfo->charset = ord(substr($this->packet, $off));
			$off += 1;
			// goto handshake_status_flags;
		}

		handshake_status_flags: {
			$this->connInfo->statusFlags = DataTypes::decode_int16(substr($this->packet, $off));
			$off += 2;
			// goto read_capability_flags2;
		}

		read_capability_flags2: {
			$this->serverCapabilities += DataTypes::decode_int16(substr($this->packet, $off)) << 16;
			$off += 2;
			// goto get_plugin_auth_data;
		}

		get_plugin_auth_data: {
			$this->authPluginDataLen = $this->serverCapabilities & self::CLIENT_PLUGIN_AUTH ? ord(substr($this->packet, $off)) : 0;
			$off += 1;
			if ($this->serverCapabilities & self::CLIENT_SECURE_CONNECTION) {
				// goto skip_reserved;
			} else {
				goto do_handshake;
			}
		}

		skip_reserved: {
			$off += 10;
			goto read_plugin_auth_data2;
		}

		read_plugin_auth_data2: {
			$strlen = max(13, $this->authPluginDataLen - 8);
			$this->authPluginData .= substr($this->packet, $off, $strlen);
			$off += $strlen;
			if ($this->serverCapabilities & self::CLIENT_PLUGIN_AUTH) {
				// goto fetch_auth_plugin_name;
			} else {
				goto do_handshake;
			}
		}

		fetch_auth_plugin_name: {
			$this->authPluginName = DataTypes::decodeNullString(substr($this->packet, $off));
			// goto do_handshake;
		}

		do_handshake: {
			$this->sendHandshake();
			$this->mysqlState = ParseState::START;
			return NULL;
		}
	}

	/** @see 14.6.4.1.2 LOCAL INFILE Request */
	private function handleLocalInfileRequest() {
		// @TODO async file fetch @rdlowrey
		$file = file_get_contents($this->packet);
		if ($file != "") {
			$this->sendPacket($file);
		}
		$this->sendPacket("");
	}

	/** @see 14.6.4.1.1 Text Resultset */
	private function handleQuery() {
		$this->parseCallback = [$this, "handleTextColumnDefinition"];
		$this->getFuture()->succeed(new ResultSet($this->connInfo, $result = new ResultProxy));
		/* we need to succeed before assigning vars, so that a when() handler won't have a partial result available */
		$this->result = $result;
		$this->result->setColumns(ord($this->packet));
	}

	/** @see 14.7.1 Binary Protocol Resultset */
	private function handleExecute() {
		$this->parseCallback = [$this, "handleBinaryColumnDefinition"];
		$this->getFuture()->succeed(new ResultSet($this->connInfo, $result = new ResultProxy));
		/* we need to succeed before assigning vars, so that a when() handler won't have a partial result available */
		$this->result = $result;
		$this->result->setColumns(ord($this->packet));
		$this->result->columns = [];
	}

	private function handleFieldList() {
		if (ord($this->packet) == self::ERR_PACKET) {
			$this->parseCallback = null;
			$this->handleError();
		} elseif (ord($this->packet) == self::EOF_PACKET) {
			$this->parseCallback = null;
			$this->parseEof();
			$this->getFuture()->succeed(null);
			$this->ready();
		} else {
			$this->getFuture()->succeed([$this->parseColumnDefinition(), $this->futures[] = new Future]);
		}
	}

	private function handleTextColumnDefinition() {
		$this->handleColumnDefinition("handleTextResultsetRow");
	}

	private function handleBinaryColumnDefinition() {
		$this->handleColumnDefinition("handleBinaryResultsetRow");
	}

	private function handleColumnDefinition($cbMethod) {
		if (!$this->result->columnsToFetch--) {
			$this->result->updateState(ResultProxy::COLUMNS_FETCHED);
			if (ord($this->packet) == self::ERR_PACKET) {
				$this->parseCallback = null;
				$this->handleError();
			} else {
				$cb = $this->parseCallback = [$this, $cbMethod];
				if ($this->capabilities & self::CLIENT_DEPRECATE_EOF) {
					$cb();
				} else {
					$this->parseEof();
					// we don't need the EOF packet, skip!
				}
			}
			return;
		}

		$this->result->columns[] = $this->parseColumnDefinition();
	}

	private function prepareParams() {
		if (!$this->result->columnsToFetch--) {
			$this->result->columnsToFetch = $this->result->columnCount;
			if (!$this->result->columnsToFetch) {
				$this->prepareFields();
			} else {
				$this->parseCallback = [$this, "prepareFields"];
			}
			return;
		}

		$this->result->params[] = $this->parseColumnDefinition();
	}

	private function prepareFields() {
		if (!$this->result->columnsToFetch--) {
			$this->parseCallback = null;
			$this->result->updateState(ResultProxy::COLUMNS_FETCHED);
			$this->query = null;
			$this->ready();

			return;
		}

		$this->result->columns[] = $this->parseColumnDefinition();
		$this->result->updateState(ResultProxy::COLUMNS_FETCHED);
	}

	/** @see 14.6.4.1.1.2 Column Defintion */
	private function parseColumnDefinition() {
		$off = 0;

		$column = [];

		if ($this->capabilities & self::CLIENT_PROTOCOL_41) {
			get_catalog: {
				$column["catalog"] = DataTypes::decodeString(substr($this->packet, $off), $intlen, $len);
				$off += $intlen + $len;
				// goto get_schema;
			}

			get_schema: {
				$column["schema"] = DataTypes::decodeString(substr($this->packet, $off), $intlen, $len);
				$off += $intlen + $len;
				// goto get_table_41;
			}

			get_table_41: {
				$column["table"] = DataTypes::decodeString(substr($this->packet, $off), $intlen, $len);
				$off += $intlen + $len;
				// goto get_original_table;
			}

			get_original_table: {
				$column["original_table"] = DataTypes::decodeString(substr($this->packet, $off), $intlen, $len);
				$off += $intlen + $len;
				// goto get_name_41;
			}

			get_name_41: {
				$column["name"] = DataTypes::decodeString(substr($this->packet, $off), $intlen, $len);
				$off += $intlen + $len;
				// goto get_original_name;
			}

			get_original_name: {
				$column["original_name"] = DataTypes::decodeString(substr($this->packet, $off), $intlen, $len);
				$off += $intlen + $len;
				// goto get_fixlen_len;
			}

			get_fixlen_len: {
				$fixlen = DataTypes::decodeInt(substr($this->packet, $off), $len);
				$off += $len;
				// goto get_fixlen;
			}

			get_fixlen: {
				$len = 0;
				$column["charset"] = DataTypes::decode_int16(substr($this->packet, $off + $len));
				$len += 2;
				$column["columnlen"] = DataTypes::decode_int32(substr($this->packet, $off + $len));
				$len += 4;
				$column["type"] = ord($this->packet[$off + $len]);
				$len += 1;
				$column["flags"] = DataTypes::decode_int16(substr($this->packet, $off + $len));
				$len += 2;
				$column["decimals"] = ord($this->packet[$off + $len]);
				$len += 1;

				$off += $fixlen;
				// goto field_fetch;
			}
		} else {
			get_table_320: {
				$column["table"] = DataTypes::decodeString(substr($this->packet, $off), $intlen, $len);
				$off += $intlen + $len;
				// goto get_name_320;
			}

			get_name_320: {
				$column["name"] = DataTypes::decodeString(substr($this->packet, $off), $intlen, $len);
				$off += $intlen + $len;
				// goto get_columnlen_len;
			}

			get_columnlen_len: {
				$collen = DataTypes::decodeInt(substr($this->packet, $off), $len);
				$off += $len;
				// goto get_columnlen;
			}

			get_columnlen: {
				$column["columnlen"] = DataTypes::decode_intByLen(substr($this->packet, $off), $collen);
				$off += $collen;
				// goto type_len;
			}

			get_type_len: {
				$typelen = DataTypes::decodeInt(substr($this->packet, $off), $len);
				$off += $len;
				// goto get_type;
			}

			get_type: {
				$column["type"] = DataTypes::decode_intByLen(substr($this->packet, $off), $typelen);
				$off += $typelen;
				// goto get_flaglen;
			}

			get_flaglen: {
				$len = 1;
				$flaglen = $this->capabilities & self::CLIENT_LONG_FLAG ? DataTypes::decodeInt(substr($this->packet, $off), $len) : ord($this->packet[$off]);
				$off += $len;
				// goto get_flags;
			}

			get_flags: {
				if ($flaglen > 2) {
					$len = 2;
					$column["flags"] = DataTypes::decode_int16(substr($this->packet, $off));
				} else {
					$len = 1;
					$column["flags"] = ord($this->packet[$off]);
				}
				$column["decimals"] = ord($this->packet[$off + $len]);
				$off += $flaglen;
				// goto field_fetch;
			}
		}

		field_fetch: {
			if ($off < $this->packetSize) {
				$column["defaults"] = DataTypes::decodeString(substr($this->packet, $off));
			}
			// goto finished;
		}

		finished: {
			return $column;
		}
	}

	/** @see 14.6.4.1.1.3 Resultset Row */
	private function handleTextResultsetRow() {
		switch ($type = ord($this->packet)) {
			case self::OK_PACKET:
				$this->parseOk();
				/* intentional fall through */
			case self::EOF_PACKET:
				if ($type == self::EOF_PACKET) {
					$this->parseEof();
				}
				$future = &$this->result->next;
				if ($this->connInfo->statusFlags & StatusFlags::SERVER_MORE_RESULTS_EXISTS) {
					$this->parseCallback = [$this, "handleQuery"];
					$this->futures[] = $future ?: $future = new Future;
				} else {
					if ($future) {
						$future->succeed(null);
					} else {
						$future = new Success(null);
					}
					$this->parseCallback = null;
				}
				$this->query = null;
				$this->ready();
				$this->result->updateState(ResultProxy::ROWS_FETCHED);
				return;
		}

		$off = 0;

		$fields = [];
		while ($off < $this->packetSize) {
			if (ord($this->packet[$off]) == 0xfb) {
				$fields[] = null;
				$off += 1;
			} else {
				$fields[] = DataTypes::decodeString(substr($this->packet, $off), $intlen, $len);
				$off += $intlen + $len;
			}
		}
		$this->result->rowFetched($fields);
	}

	/** @see 14.7.2 Binary Protocol Resultset Row */
	private function handleBinaryResultsetRow() {
		if (ord($this->packet) == self::EOF_PACKET) {
			$this->parseEof();
			$future = &$this->result->next;
			if ($this->connInfo->statusFlags & StatusFlags::SERVER_MORE_RESULTS_EXISTS) {
				$this->parseCallback = [$this, "handleQuery"];
				$this->futures[] = $future ?: $future = new Future;
			} else {
				if ($future) {
					$future->succeed(null);
				} else {
					$future = new Success(null);
				}
				$this->parseCallback = null;
			}
			$this->query = null;
			$this->ready();
			$this->result->updateState(ResultProxy::ROWS_FETCHED);
			return;
		}

		$off = 1; // skip first byte

		$columnCount = $this->result->columnCount;
		$columns = $this->result->columns;
		$fields = [];

		for ($i = 0; $i < $columnCount; $i++) {
			if (ord($this->packet[$off + (($i + 2) >> 3)]) & (1 << (($i + 2) % 8))) {
				$fields[$i] = null;
			}
		}
		$off += ($columnCount + 9) >> 3;

		for ($i = 0; $off < $this->packetSize; $i++) {
			while (array_key_exists($i, $fields)) $i++;
			$fields[$i] = DataTypes::decodeBinary($columns[$i]["type"], substr($this->packet, $off), $len);
			$off += $len;
		}
		ksort($fields);
		$this->result->rowFetched($fields);
	}

	/** @see 14.7.4.1 COM_STMT_PREPARE Response */
	private function handlePrepare() {
		switch (ord($this->packet)) {
			case self::OK_PACKET:
				break;
			case self::ERR_PACKET:
				$this->handleError();
				break;
			default:
				throw new \UnexpectedValueException("Unexpected value for first byte of COM_STMT_PREPARE Response");
		}
		$off = 1;

		stmt_id: {
			$stmtId = DataTypes::decode_int32(substr($this->packet, $off));
			$off += 4;

			// goto get_columns;
		}

		get_columns: {
			$columns = DataTypes::decode_int16(substr($this->packet, $off));
			$off += 2;

			// gotoo get_params;
		}

		get_params: {
			$params = DataTypes::decode_int16(substr($this->packet, $off));
			$off += 2;

			// goto skip_filler;
		}

		skip_filler: {
			$off += 1;

			// goto warning_count;
		}

		warning_count: {
			$this->connInfo->warnings = DataTypes::decode_int16(substr($this->packet, $off));

			// goto finish;
		}

		finish: {
			$this->named = [];
			$this->result = new ResultProxy;
			$this->result->columnsToFetch = $params;
			$this->result->columnCount = $columns;
			$this->result->columns = [];
			$this->getFuture()->succeed(new Stmt($this, $this->query, $stmtId, $this->named, $this->result));
			if ($params) {
				$this->parseCallback = [$this, "prepareParams"];
			} else {
				$this->prepareParams();
			}
		}
	}

	private function readStatistics() {
		$this->getFuture()->succeed($this->packet);
		$this->ready();
		$this->parseCallback = null;
	}

	private function closeSocket() {
		if ($this->readWatcher) {
			$this->reactor->cancel($this->readWatcher);
			$this->readWatcher = null;
			fclose($this->socket);
		}
		if ($this->writeWatcher) {
			$this->reactor->cancel($this->writeWatcher);
			$this->writeWatcher = null;
		}
		$this->connectionState = self::CLOSED;
	}

	private function compilePacket() {
		 do {
			$pending = current($this->out);
			unset($this->out[key($this->out)]);
			if ($pending !== null || empty($this->out)) {
				break;
			}
			$this->seqId = $this->compressionId = -1;
		} while (1);
		if ($pending == "") {
			return $pending;
		}

		$packet = "";
		do {
			$len = strlen($pending);
			if ($len >= (1 << 24) - 1) {
				$out = substr($pending, 0, (1 << 24) - 1);
				$pending = substr($pending, (1 << 24) - 1);
				$len = (1 << 24) - 1;
			} else {
				$out = $pending;
				$pending = "";
			}
			$packet .= substr_replace(pack("V", $len), chr(++$this->seqId), 3, 1) . $out; // expects $len < (1 << 24) - 1
		} while ($pending != "");

		if (defined("MYSQL_DEBUG")) {
			print "out: ";
			for ($i = 0; $i < min(strlen($packet), 200); $i++)
				fwrite(STDERR, dechex(ord($packet[$i])) . " ");
			$r = range("\0", "\x1f");
			unset($r[10], $r[9]);
			print "len: ".strlen($packet)." ";
			var_dump(str_replace($r, ".", substr($packet, 0, 200)));
		}

		return $packet;
	}

	private function compressPacket($packet) {
		$packet = $this->uncompressedOut.$packet;

		if ($packet == "") {
			return "";
		}

		$len = strlen($packet);
		while ($len < self::MAX_UNCOMPRESSED_BUFLEN && !empty($this->out)) {
			$packet .= $this->compilePacket();
			$len = strlen($this->uncompressedOut);
		}

		$this->uncompressedOut = substr($packet, self::MAX_UNCOMPRESSED_BUFLEN);
		$packet = substr($packet, 0, self::MAX_UNCOMPRESSED_BUFLEN);
		$len = strlen($packet);

		$deflated = zlib_encode($packet, ZLIB_ENCODING_DEFLATE);
		if ($len < strlen($deflated)) {
			$out = substr_replace(pack("V", strlen($packet)), chr(++$this->compressionId), 3, 1) . "\0\0\0" . $packet;
		} else {
			$out = substr_replace(pack("V", strlen($deflated)), chr(++$this->compressionId), 3, 1) . substr(pack("V", $len), 0, 3) . $deflated;
		}

		return $out;
	}

	public function onWrite($reactor, $watcherId, $socket) {
		if ($this->outBuflen == 0) {
			$doCompress = ($this->capabilities & self::CLIENT_COMPRESS) && $this->connectionState >= self::READY;

			$packet = $this->compilePacket();
			if ($doCompress) {
				$packet = $this->compressPacket($packet);
			}

			$this->outBuf = $packet;
			$this->outBuflen = strlen($packet);

			if ($this->outBuflen == 0) {
				$reactor->disable($watcherId);
				$this->watcherEnabled = false;
			}
		}

		$bytes = @fwrite($socket, $this->outBuf);
		$this->outBuflen -= $bytes;
		if ($this->outBuflen > 0) {
			if ($bytes == 0) {
				$this->goneAway();
			} else {
				$this->outBuf = substr($this->outBuf, $bytes);
			}
		}
	}

	public function onRead() {
		$bytes = @fread($this->socket, $this->readGranularity);
		if ($bytes != "") {
			if (($this->capabilities & self::CLIENT_COMPRESS) && $this->connectionState >= self::READY) {
				$bytes = $this->parseCompression($bytes);
			}
			if ($bytes != "") {
				$this->parseMysql($bytes);
			}
		} else {
			$this->goneAway();
		}
	}

	private function goneAway() {
		foreach ($this->futures as $future) {
			if ($this->config->exceptions || $this->connectionState < self::READY) {
				if ($this->query == "") {
					$future->fail(new InitializationException("Connection went away"));
				} else {
					$future->fail(new QueryException("Connection went away... unable to fulfil this future ... It's unknown whether the query was executed...", $this->query));
				}
			} else {
				$future->succeed(false);
			}
		}
		$this->closeSocket();
		if (null !== $cb = $this->config->restore) {
			$cb($this, $this->connectionState < self::READY);
			/* @TODO if packet not completely sent, resend? */
		}
	}

	/** @see 14.4 Compression */
	private function parseCompression($inBuf) {
		$this->compressionBuf .= $inBuf;
		$this->compressionBuflen += strlen($inBuf);
		$inflated = "";

		start: {
			switch ($this->compressionState) {
				case ParseState::START:
					goto determine_header;
				case ParseState::FETCH_PACKET:
					goto fetch_packet;
				default:
					throw new \Exception("{$this->compressionState} is not a valid ParseState constant");
			}
		}

		determine_header: {
			if ($this->compressionBuflen < 4) {
				goto more_data_needed;
			}

			$this->compressionSize = DataTypes::decode_int24($this->compressionBuf);

			$this->compressionId = ord($this->mysqlBuf[3]);

			$this->uncompressedSize = DataTypes::decode_int24(substr($this->compressionBuf, 4, 3));

			$this->compressionBuf = substr($this->compressionBuf, 7);
			$this->compressionBuflen -= 7;
			$this->compressionState = ParseState::FETCH_PACKET;

			// goto fetch_packet;
		}

		fetch_packet: {
			if ($this->compressionBuflen < $this->compressionSize) {
				goto more_data_needed;
			}

			if ($this->compressionSize > 0) {
				if ($this->uncompressedSize == 0) {
					$inflated .= substr($this->compressionBuf, 0, $this->compressionSize);
				} else {
					$inflated .= zlib_decode(substr($this->compressionBuf, 0, $this->compressionSize), $this->uncompressedSize);
				}
				$this->compressionBuf = substr($this->compressionBuf, $this->compressionSize);
				$this->compressionBuflen -= $this->compressionSize;
			}

			// goto finished;
		}

		finished: {
			$this->compressionState = ParseState::START;
			if ($this->compressionBuflen > 0) {
				goto start;
			}
			return $inflated;
		}

		more_data_needed: {
			return $inflated;
		}
	}

	/**
	 * @see 14.1.2 MySQL Packet
	 * @see 14.1.3 Generic Response Packets
	 */
	private function parseMysql($inBuf) {
		$this->mysqlBuf .= $inBuf;
		$this->mysqlBuflen += strlen($inBuf);

		start: {
			switch ($this->mysqlState) {
				case ParseState::START:
					goto determine_header;
				case ParseState::FETCH_PACKET:
					goto fetch_packet;
				default:
					throw new \Exception("{$this->mysqlState} is not a valid ParseState constant");
			}
		}

		determine_header: {
			if ($this->mysqlBuflen < 4) {
				goto more_data_needed;
			}

			$len = DataTypes::decode_int24($this->mysqlBuf);

			if ($this->lastIn) {
				$this->packet = "";
				$this->packetSize = $len;
			} else {
				$this->packetSize += $len;
			}
			$this->lastIn = $len != 0xffffff;

			$this->seqId = ord($this->mysqlBuf[3]);

			$this->mysqlBuf = substr($this->mysqlBuf, 4);
			$this->mysqlBuflen -= 4;
			$this->mysqlState = ParseState::FETCH_PACKET;

			// goto fetch_packet;
		}

		fetch_packet: {
			if ($this->mysqlBuflen < ($this->packetSize & 0xffffff)) {
				goto more_data_needed;
			}

			if ($this->lastIn) {
				$size = $this->packetSize % 0xffffff;
			} else {
				$size = 0xffffff;
			}

			$this->packet .= substr($this->mysqlBuf, 0, $size);
			$this->mysqlBuf = substr($this->mysqlBuf, $size);
			$this->mysqlBuflen -= $size;

			if (!$this->lastIn) {
				$this->mysqlState = ParseState::START;
				goto determine_header;
			}

			if ($this->packetSize > 0) {
				if (defined("MYSQL_DEBUG")) {
					print "in: ";
					$print = substr_replace(pack("V", $this->packetSize), chr($this->seqId), 3, 1);
					for ($i = 0; $i < 4; $i++)
						fwrite(STDERR, dechex(ord($print[$i])) . " ");
					for ($i = 0; $i < min(200, $this->packetSize); $i++)
						fwrite(STDERR, dechex(ord($this->packet[$i])) . " ");
					$r = range("\0", "\x1f");
					unset($r[10], $r[9]);
					print "len: ".strlen($this->packet)." ";
					var_dump(str_replace($r, ".", substr($this->packet, 0, 200)));
				}
				if ($this->parseCallback) {
					$cb = $this->parseCallback;
					$cb();
					goto finished;
				} else {
					$this->packetType = ord($this->packet);
					// goto payload;
				}
			}
		}

		payload: {
			$cb = $this->packetCallback;
			$this->packetCallback = null;
			switch ($this->packetType) {
				case self::OK_PACKET:
					$this->connectionState = self::READY;
					$this->handleOk();
					break;
				case self::LOCAL_INFILE_REQUEST:
					$this->handleLocalInfileRequest();
					break;
				case self::ERR_PACKET:
					$this->handleError();
					break;
				case self::EOF_PACKET:
					if ($this->packetSize < 6) {
						$this->handleEof();
						break;
					}
					/* intentionally missing break */
				case self::EXTRA_AUTH_PACKET:
					if ($this->connectionState === self::ESTABLISHED) {
						/** @see 14.2.5 Connection Phase Packets (AuthMoreData) */
						switch ($this->authPluginName) {
							case "sha256_password":
								$key = substr($this->packet, 1);
								$this->config->key = $key;
								$this->sendHandshake();
								break;
							default:
								throw new \UnexpectedValueException("Unexpected EXTRA_AUTH_PACKET in authentication phase for method {$this->authPluginName}");
						}
						break;
					}
					/* intentionally missing break */
				default:
					if ($this->writeWatcher === NULL) {
						$this->established();
						$this->handleHandshake();
					} elseif ($cb) {
						$cb();
					} else {
						throw new \UnexpectedValueException("Unexpected packet type: {$this->packetType}");
					}
			}
			goto finished;
		}

		finished: {
			$this->mysqlState = ParseState::START;
			if ($this->mysqlBuflen > 0) {
				goto start;
			}
			return true;
		}

		more_data_needed: {
			return NULL;
		}
	}

	private function secureAuth($pass, $scramble) {
		$hash = sha1($pass, 1);
		return $hash ^ sha1(substr($scramble, 0, 20) . sha1($hash, 1), 1);
	}

	private function sha256Auth($pass, $scramble, $key) {
		openssl_public_encrypt($pass ^ str_repeat($scramble, ceil(strlen($pass) / strlen($scramble))), $auth, $key, OPENSSL_PKCS1_OAEP_PADDING);
		return $auth;
	}

	private function authSwitchRequest() {
		$this->parseCallback = null;
		switch (ord($this->packet)) {
			case self::EOF_PACKET:
				if ($this->packetSize == 1) {
					break;
				}
				$len = strpos($this->packet, "\0");
				$pluginName = substr($this->packet, 0, $len); // @TODO mysql_native_pass only now...
				$authPluginData = substr($this->packet, $len + 1);
				$this->sendPacket($this->secureAuth($this->config->pass, $authPluginData));
				break;
			case self::ERR_PACKET:
				$this->handleError();
				return;
			default:
				throw new \UnexpectedValueException("AuthSwitchRequest: Expecting 0xfe (or ERR_Packet), got 0x".dechex(ord($this->packet)));
		}
	}

	/**
	 * @see 14.2.5 Connection Phase Packets
	 * @see 14.3 Authentication Method
	 */
	private function sendHandshake($inSSL = false) {
		if ($this->config->db !== null) {
			$this->capabilities |= self::CLIENT_CONNECT_WITH_DB;
		}

		if ($this->config->ssl !== null) {
			$this->capabilities |= self::CLIENT_SSL;
		}

		$this->capabilities &= $this->serverCapabilities;

		$payload = "";
		$payload .= pack("V", $this->capabilities);
		$payload .= pack("V", 1 << 24 - 1); // max-packet size
		$payload .= chr($this->config->binCharset);
		$payload .= str_repeat("\0", 23); // reserved

		if (!$inSSL && ($this->capabilities & self::CLIENT_SSL)) {
			$this->_sendPacket($payload);
			$this->reactor->onWritable($this->socket, function ($reactor, $watcherId, $socket) {
				/* wait until main write watcher has written everything... */
				if ($this->outBuflen > 0 || !empty($this->out)) {
					return;
				}

				$this->reactor->cancel($watcherId);
				$this->reactor->disable($this->readWatcher);
				(new \Nbsock\Encryptor($reactor))->enable($socket, $this->config->ssl + ['peer_name' => $this->config->host])->when(function ($error) {
					if ($error) {
						$this->getFuture()->fail($error);
						$this->closeSocket();
						return;
					}

					$this->reactor->enable($this->readWatcher);
					$this->sendHandshake(true);
				});
			});
			return;
		}

		$payload .= $this->config->user."\0";
		if ($this->config->pass == "") {
			$auth = "";
		} elseif ($this->capabilities & self::CLIENT_PLUGIN_AUTH) {
			switch ($this->authPluginName) {
				case "mysql_native_password":
					$auth = $this->secureAuth($this->config->pass, $this->authPluginData);
					break;
				case "mysql_clear_password":
					$auth = $this->config->pass;
					break;
				case "sha256_password":
					if ($this->config->pass === "") {
						$auth = "";
					} else {
						if (isset($this->config->key)) {
							$auth = $this->sha256Auth($this->config->pass, $this->authPluginData, $this->config->key);
						} else {
							$auth = "\x1";
						}
					}
					break;
				case "mysql_old_password":
					throw new \UnexpectedValueException("mysql_old_password is outdated and insecure. Intentionally not implemented!");
				default:
					throw new \UnexpectedValueException("Invalid (or unimplemented?) auth method requested by server: {$this->authPluginName}");
			}
		} else {
			$auth = $this->secureAuth($this->config->pass, $this->authPluginData);
		}
		if ($this->capabilities & self::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) {
			$payload .= DataTypes::encodeInt(strlen($auth));
			$payload .= $auth;
		} elseif ($this->capabilities & self::CLIENT_SECURE_CONNECTION) {
			$payload .= chr(strlen($auth));
			$payload .= $auth;
		} else {
			$payload .= "$auth\0";
		}
		if ($this->capabilities & self::CLIENT_CONNECT_WITH_DB) {
			$payload .= "{$this->config->db}\0";
		}
		if ($this->capabilities & self::CLIENT_PLUGIN_AUTH) {
			$payload .= "\0"; // @TODO AUTH
//			$payload .= "mysql_native_password\0";
		}
		if ($this->capabilities & self::CLIENT_CONNECT_ATTRS) {
			// connection attributes?! 5.6.6+ only!
		}
		$this->_sendPacket($payload);
	}

	/** @see 14.1.2 MySQL Packet */
	private function sendPacket($payload) {
		if ($this->connectionState !== self::READY) {
			throw new \Exception("Connection not ready, cannot send any packets");
		}

		$this->_sendPacket($payload);
	}

	private function _sendPacket($payload) {
		$this->out[] = $payload;
		if (!$this->watcherEnabled) {
			$this->reactor->enable($this->writeWatcher);
			$this->watcherEnabled = true;
		}
	}
}