<?php

namespace Mysql;
use Amp\Future;
use Amp\Reactor;
use Amp\Success;
use Nbsock\Connector;

/* @TODO
 * Character Set(s)
 * CLIENT_SSL (14.2.5 / 14.5)
 * fallback old ath?
 * Auth switch request??
 * COM_CHANGE_USER
 * 14.3 alternative auths
 * use better Exceptions...
 */

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
	private $readWatcher;
	private $writeWatcher = NULL;
	private $watcherEnabled = false;
	private $authPluginDataLen;
	private $query;
	private $parseCallback = null;
	private $packetCallback = null;

	private $reactor;
	private $connector;
	private $config;
	private $futures = [];
	private $onReady = [];
	private $resultSet = null;
	private $resultSetMethod;
	private $host;
	private $resolvedHost;
	private $user;
	private $pass;
	private $db;
	private $oldDb = NULL;

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
	const ERR_PACKET = 0xff;
	const EOF_PACKET = 0xfe;

	const UNCONNECTED = 0;
	const ESTABLISHED = 1;
	const READY = 2;
	const QUITTING = 3;
	const CLOSED = 4;

	public function __construct(Reactor $reactor, Connector $connector, ConnectionConfig $config, $host, $resolvedHost, $user, $pass, $db = null) {
		$this->reactor = $reactor;
		$this->connector = $connector;
		$this->host = $host;
		$this->resolvedHost = $resolvedHost;
		$this->user = $user;
		$this->pass = $pass;
		$this->db = $db;
		$this->config = $config;
		$this->connInfo = new ConnectionState;
	}

	public function alive() {
		return $this->connectionState == self::READY;
	}

	public function close() {
		$this->closeSocket();
	}

	public function getConfig() {
		return $this->config;
	}

	private function ready() {
		if (empty($this->futures)) {
			if (empty($this->onReady)) {
				$cb = $this->config->ready;
			} else {
				list($key, $cb) = each($this->onReady);
				unset($this->onReady[$key]);
			}
			if (isset($cb) && is_callable($cb)) {
				$cb($this);
			}
		}
	}

	public function connect() {
		$future = new Future($this->reactor);
		$this->connector->connect($this->resolvedHost)->when(function ($error, $socket) use ($future) {
			if ($error) {
				$future->fail($error);
			} else {
				$this->socket = $socket;
				$this->readWatcher = $this->reactor->onReadable($this->socket, [$this, "onInit"]);
				$this->futures[] = $future;
			}
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
		if ($this->packetCallback || $this->parseCallback || !empty($this->onReady)) {
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

	private function startCommand($future = null) {
		$this->seqId = $this->compressionId = -1;
		/*$payload = array_pop($this->out);
		$this->out[] = null;
		$this->out[] = $payload;*/
		return $this->futures[] = $future ?: new Future($this->reactor);
	}

	private function setCharset($charset, $collate) {
		return $this->query("SET NAMES '$charset' COLLATE '$collate'");
	}

	/** @see 14.6.2 COM_QUIT */
	public function closeConnection($future = null) {
		$this->sendPacket("\x01");
		$this->connectionState = self::QUITTING;
		return $this->startCommand($future);
	}

	/** @see 14.6.3 COM_INIT_DB */
	public function useDb($db, $future = null) {
		$this->oldDb = $this->db;
		$this->db = $db;
		$this->sendPacket("\x02$db");
		return $this->startCommand($future);
	}

	/** @see 14.6.4 COM_QUERY */
	public function query($query, $future = null) {
		$this->sendPacket("\x03$query");
		$this->packetCallback = [$this, "handleQuery"];
		return $this->startCommand($future);
	}

	/** @see 14.6.5 COM_FIELD_LIST */
	public function listFields($table, $like = "%", $future = null) {
		$this->sendPacket("\x04$table\0$like");
		$this->parseCallback = [$this, "handleFieldlist"];
		return $this->startCommand($future);
	}

	public function listAllFields($table, $like = "%", $future = null) {
		if (!$future) {
			$future = new Future($this->reactor);
		}

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
	public function createDatabase($db, $future = null) {
		$this->sendPacket("\x05$db");
		return $this->startCommand($future);
	}

	/** @see 14.6.7 COM_DROP_DB */
	public function dropDatabase($db, $future = null) {
		$this->sendPacket("\x06$db");
		return $this->startCommand($future);
	}

	/** @see 14.6.8 COM_REFRESH */
	public function refresh($subcommand, $future = null) {
		$this->sendPacket("\x07$subcommand");
		return $this->startCommand($future);
	}

	/** @see 14.6.9 COM_SHUTDOWN */
	public function shutdown($future = null) {
		$this->sendPacket("\x08\x00"); /* SHUTDOWN_DEFAULT / SHUTDOWN_WAIT_ALL_BUFFERS, only one in use */
		return $this->startCommand($future);
	}

	/** @see 14.6.10 COM_STATISTICS */
	public function statistics($future = null) {
		$this->sendPacket("\x09");
		$this->parseCallback = [$this, "readStatistics"];
		return $this->startCommand($future);
	}

	/** @see 14.6.11 COM_PROCESS_INFO */
	public function processInfo($future = null) {
		$this->sendPacket("\x0a");
		$this->packetCallback = [$this, "handleQuery"];
		return $this->startCommand($future);
	}

	/** @see 14.6.13 COM_PROCESS_KILL */
	public function killProcess($process, $future = null) {
		$this->sendPacket("\x0c$process");
		return $this->startCommand($future);
	}

	/** @see 14.6.14 COM_DEBUG */
	public function debugStdout($future = null) {
		$this->sendPacket("\x0d");
		return $this->startCommand($future);
	}

	/** @see 14.6.15 COM_PING */
	public function ping($future = null) {
		$this->sendPacket("\x0d");
		return $this->startCommand($future);
	}

	/** @see 14.6.18 COM_CHANGE_USER */
	/* @TODO broken, my test server doesn't support that command, can't test now
	public function changeUser($user, $pass, $db = null, $future = null) {
		$this->user = $user;
		$this->pass = $pass;
		$this->db = $db;
		$payload = "\x11";

		$payload .= "$user\0";
		$auth = $this->secureAuth($this->pass, $this->authPluginData);
		if ($this->capabilities & self::CLIENT_SECURE_CONNECTION) {
			$payload .= ord($auth).$auth;
		} else {
			$payload .= "$auth\0";
		}
		$payload .= "$db\0";

		$this->sendPacket($payload);
		$this->parseCallback = [$this, "authSwitchRequest"];
		return $this->startCommand($future);
	}
	*/

	/** @see 14.6.19 COM_RESET_CONNECTION */
	public function resetConnection($future = null) {
		$this->sendPacket("\x1f");
		return $this->startCommand($future);
	}

	/** @see 14.7.4 COM_STMT_PREPARE */
	public function prepare($query, $future = null) {
		$this->query = $query;
		$this->sendPacket("\x16$query");
		$this->parseCallback = [$this, "handlePrepare"];
		return $this->startCommand($future);
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
	// @TODO what to do with the prebound params?! (bindParam())
	/* prebound params: null-bit set, type MYSQL_TYPE_LONG_BLOB, no value */
	public function execute($stmtId, &$params, $prebound, $data = []) {
		$future = new Future($this->reactor);
		$this->appendTask(function () use ($stmtId, &$params, $prebound, $data, $future) {
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
			}
			$payload .= chr($bound);
			if ($bound) {
				$payload .= $types;
				$payload .= $values;
			}

			$this->out[] = null;
			$this->futures[] = $future;
			$this->sendPacket($payload);
			$this->packetCallback = [$this, "handleExecute"];
		});
		return $future; // do not use $this->startCommand(), that might unexpectedly reset the seqId!
	}

	public function closeStmt($stmtId) {
		$payload = "\x19";
		$payload .= DataTypes::encode_int32($stmtId);
		$this->appendTask(function () use ($payload) {
			if ($this->connectionState === self::READY) {
				$this->out[] = null;
				$this->sendPacket($payload);
			}
			$this->ready();
		});
	}

	public function fetchStmt($stmtId) {
		$payload = "\x1c";
		$payload .= DataTypes::encode_int32($stmtId);
		$payload .= DataTypes::encode_int32(1);
		$future = new Future($this->reactor);
		$this->appendTask(function () use ($payload, $future) {
			$this->out[] = null;
			$this->futures[] = $future;
			$this->sendPacket($payload);
			$this->ready();
		});
		return $future;
	}

	public function resetStmt($stmtId) {
		$payload = "\x1a";
		$payload .= DataTypes::encode_int32($stmtId);
		$future = new Future($this->reactor);
		$this->appendTask(function () use ($payload, $future) {
			$this->out[] = null;
			$this->futures[] = $future;
			$this->sendPacket($payload);
		});
		return $future;
	}

	private function established() {
		// @TODO flags to use?
		$this->capabilities |= self::CLIENT_SESSION_TRACK | self::CLIENT_TRANSACTIONS | self::CLIENT_PROTOCOL_41 | self::CLIENT_SECURE_CONNECTION | self::CLIENT_MULTI_RESULTS | self::CLIENT_PS_MULTI_RESULTS | self::CLIENT_MULTI_STATEMENTS;

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
					$this->getFuture()->fail(new \Exception("MySQL error ({$this->connInfo->errorCode}): {$this->connInfo->errorState} {$this->connInfo->errorMsg}"));
				} else {
					$this->getFuture()->succeed(false);
				}
				$this->ready();
			} elseif ($this->connectionState == self::ESTABLISHED) {
				// connection failure
				$this->closeSocket();
				$this->getFuture()->fail(new \Exception("Could not connect to {$this->resolvedHost}: {$this->connInfo->errorState} {$this->connInfo->errorMsg}"));
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
				throw new \Exception("Unsupported protocol version ".ord($this->packet)." (Expected: 10)");
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
		// @TODO split over multiple packets
		$file = file_get_contents($this->packet);
		if ($file != "") {
			$this->sendPacket($file);
		}
		$this->sendPacket("");
	}

	private function &resultSet($prop, $val = NAN) {
		$cb = $this->resultSet;
		return $cb($prop, $val);
	}

	private function resultSetMethod($method) {
		$args = func_get_args();
		unset($args[0]);
		$cb = $this->resultSetMethod;
		return $cb($method, $args);
	}

	private function bindResultSet($resultSet) {
		$class = get_class($resultSet);
		$this->resultSet = \Closure::bind(function &($prop, $val = NAN) { if (!@is_nan($val)) $this->$prop = $val; return $this->$prop; }, $resultSet, $class);
		$this->resultSetMethod = \Closure::bind(function ($method, $args) { call_user_func_array([$this, $method], $args); }, $resultSet, $class);
	}

	/** @see 14.6.4.1.1 Text Resultset */
	private function handleQuery() {
		$this->parseCallback = true; // ensure that parseCallback is set for verification in self::appendTask()
		$this->getFuture()->succeed($resultSet = new ResultSet($this->reactor, $this->connInfo));
		$this->bindResultSet($resultSet);
		$this->parseCallback = [$this, "handleTextColumnDefinition"];
		$this->resultSetMethod("setColumns", ord($this->packet));
	}

	/** @see 14.7.1 Binary Protocol Resultset */
	private function handleExecute() {
		$this->parseCallback = true; // ensure that parseCallback is set for verification in self::appendTask()
		$this->getFuture()->succeed($resultSet = new ResultSet($this->reactor, $this->connInfo));
		$this->bindResultSet($resultSet);
		$this->parseCallback = [$this, "handleBinaryColumnDefinition"];
		$this->resultSetMethod("setColumns", ord($this->packet));
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
			$this->getFuture()->succeed([$this->parseColumnDefinition(), $this->futures[] = new Future($this->reactor)]);
		}
	}

	private function handleTextColumnDefinition() {
		$this->handleColumnDefinition("handleTextResultsetRow");
	}

	private function handleBinaryColumnDefinition() {
		$this->handleColumnDefinition("handleBinaryResultsetRow");
	}

	private function handleColumnDefinition($cbMethod) {
		$toFetch = &$this->resultSet("columnsToFetch");
		if (!$toFetch--) {
			$this->resultSetMethod("updateState", ResultSet::COLUMNS_FETCHED);
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

		$this->resultSet("columns")[] = $this->parseColumnDefinition();
	}

	private function prepareParams() {
		$toFetch = &$this->resultSet("columnsToFetch");
		if (!$toFetch--) {
			//$this->resultSetMethod("updateState", ResultSet::PARAMS_FETCHED);
			$toFetch = $this->resultSet("columnCount");
			if (!$toFetch) {
				$this->prepareFields();
			} else {
				$this->parseCallback = [$this, "prepareFields"];
			}
			return;
		}

		$this->resultSet("params")[] = $this->parseColumnDefinition();
		//$this->resultSetMethod("updateState", ResultSet::PARAMS_FETCHED);
	}

	private function prepareFields() {
		$toFetch = &$this->resultSet("columnsToFetch");
		if (!$toFetch--) {
			$this->parseCallback = null;
			$this->resultSetMethod("updateState", ResultSet::COLUMNS_FETCHED);
			$this->ready();

			return;
		}

		$this->resultSet("columns")[] = $this->parseColumnDefinition();
		$this->resultSetMethod("updateState", ResultSet::COLUMNS_FETCHED);
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
				$future = &$this->resultSet("next");
				if ($this->connInfo->statusFlags & StatusFlags::SERVER_MORE_RESULTS_EXISTS) {
					$this->parseCallback = [$this, "handleQuery"];
					$this->futures[] = $future ?: $future = new Future($this->reactor);
				} else {
					if ($future) {
						$future->succeed(null);
					} else {
						$future = new Success(null);
					}
					$this->parseCallback = null;
				}
				$this->ready();
				$this->resultSetMethod("updateState", ResultSet::ROWS_FETCHED);
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
		$this->resultSetMethod("rowFetched", $fields);
	}

	/** @see 14.7.2 Binary Protocol Resultset Row */
	private function handleBinaryResultsetRow() {
		if (ord($this->packet) == self::EOF_PACKET) {
			$this->parseEof();
			$future = &$this->resultSet("next");
			if ($this->connInfo->statusFlags & StatusFlags::SERVER_MORE_RESULTS_EXISTS) {
				$this->parseCallback = [$this, "handleQuery"];
				$this->futures[] = $future ?: $future = new Future($this->reactor);
			} else {
				if ($future) {
					$future->succeed(null);
				} else {
					$future = new Success(null);
				}
				$this->parseCallback = null;
			}
			$this->ready();
			$this->resultSetMethod("updateState", ResultSet::ROWS_FETCHED);
			return;
		}

		$off = 1; // skip first byte

		$columnCount = $this->resultSet("columnCount");
		$columns = $this->resultSet("columns");
		$fields = [];

		for ($i = 0; $i < $columnCount; $i++) {
			if (ord($this->packet[$off + (($i + 2) >> 3)]) & (($i + 2) % 8)) {
				$fields[$i] = null;
			}
		}
		$off += ($columnCount + 9) >> 3;

		for ($i = 0; $off < $this->packetSize; $i++) {
			$fields[] = DataTypes::decodeBinary($columns[$i]["type"], substr($this->packet, $off), $len);
			$off += $len;
		}
		$this->resultSetMethod("rowFetched", $fields);
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
			$resultset = new Stmt($this->reactor, $this, $this->query, $stmtId, $columns, $params);
			$this->bindResultSet($resultset);
			$this->getFuture()->succeed($resultset);
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
		$this->reactor->cancel($this->readWatcher);
		if ($this->writeWatcher) {
			$this->reactor->cancel($this->writeWatcher);
			$this->writeWatcher = null;
		}
		fclose($this->socket);
		$this->connectionState = self::CLOSED;
	}

	private function compilePacket() {
		while (1) {
			$pending = current($this->out);
			unset($this->out[key($this->out)]);
			if ($pending !== null || empty($this->out)) {
				break;
			}
			$this->seqId = $this->compressionId = -1;
		}
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
			for ($i = 0; $i < min(strlen($packet), 200); $i++)
				fwrite(STDERR, dechex(ord($packet[$i])) . " ");
			$r = range("\0", "\x19");
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

	public function onWrite() {
		if ($this->outBuflen == 0) {
			$doCompress = ($this->capabilities & self::CLIENT_COMPRESS) && $this->connectionState >= self::READY;

			$packet = $this->compilePacket();
			if ($doCompress) {
				$packet = $this->compressPacket($packet);
			}

			$this->outBuf = $packet;
			$this->outBuflen = strlen($packet);

			if ($this->outBuflen == 0) {
				$this->reactor->disable($this->writeWatcher);
				$this->watcherEnabled = false;
			}
		}

		$bytes = @fwrite($this->socket, $this->outBuf);
		$this->outBuflen -= $bytes;
		if ($this->outBuflen > 0) {
			if ($bytes == 0) {
				// @TODO handle gone away
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
			// Gone away...
			// @TODO restart connection; throw error? remove from ready Connections
			var_dump("Gone away?!");
			$this->closeSocket();
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
					throw new \UnexpectedValueException("{$this->compressionState} is not a valid ParseState constant");
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
					throw new \UnexpectedValueException("{$this->mysqlState} is not a valid ParseState constant");
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
					$print = substr_replace(pack("V", $this->packetSize), chr($this->seqId), 3, 1);
					for ($i = 0; $i < 4; $i++)
						fwrite(STDERR, dechex(ord($print[$i])) . " ");
					for ($i = 0; $i < min(200, $this->packetSize); $i++)
						fwrite(STDERR, dechex(ord($this->packet[$i])) . " ");
					print "len: ".strlen($this->packet)." ";
					var_dump(substr($this->packet, 0, 200));
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
					if ($this->mysqlState === self::ESTABLISHED) {
						/** @see 14.2.5 Connection Phase Packets (AuthMoreData) */
						// @TODO ... 14.2.2.2
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

	private function authSwitchRequest() {
		$this->parseCallback = null;
		switch (ord($this->packet)) {
			case 0xfe:
				if ($this->packetSize == 1) {
					break;
				}
				$len = strpos($this->packet, "\0");
				$pluginName = substr($this->packet, 0, $len); // @TODO mysql_native_pass only now...
				$authPluginData = substr($this->packet, $len + 1);
				$this->sendPacket($this->secureAuth($this->pass, $authPluginData));
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
	private function sendHandshake() {
		if ($this->db !== null) {
			$this->capabilities |= self::CLIENT_CONNECT_WITH_DB;
		}

		$this->capabilities &= $this->serverCapabilities;

		$payload = "";
		$payload .= pack("V", $this->capabilities);
		$payload .= pack("V", 1 << 24 - 1); // max-packet size
		$payload .= chr($this->config->charset);
		$payload .= str_repeat("\0", 23); // reserved
		$payload .= $this->user."\0";
		if ($this->capabilities & self::CLIENT_PLUGIN_AUTH) {
			$auth = ""; // @TODO AUTH
		} elseif ($this->pass !== "") {
			$auth = $this->secureAuth($this->pass, $this->authPluginData);
		} else {
			$auth = "";
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
			$payload .= "{$this->db}\0";
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