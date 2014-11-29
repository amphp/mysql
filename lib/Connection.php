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
 * 14.4 Compression
 * option for exceptions
 * large packets (>= 1 << 24 bytes)
 */

class Connection {
	private $out = [];
	private $outBuf;
	private $outBuflen = 0;
	private $inBuf;
	private $inBuflen = 0;
	private $packet;
	private $unreadlen;
	private $state = ParseState::START;
	private $oldState;
	private $protocol;
	private $seqId = -1;
	private $packetSize;
	private $packetType;
	private $readLen = 0;
	private $strlen;
	private $socket;
	private $readGranularity = 4096;
	private $readWatcher;
	private $writeWatcher = NULL;
	private $watcherEnabled = false;
	private $authPluginDataLen;
	private $parseCallback = null;
	private $packetCallback = null;

	private $reactor;
	private $connector;
	private $ready;
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

	const CLIENT_LONG_FLAG = 0x00000004;
	const CLIENT_CONNECT_WITH_DB = 0x00000008;
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

	public function __construct(Reactor $reactor, Connector $connector, callable $ready, $host, $resolvedHost, $user, $pass, $db = null) {
		$this->reactor = $reactor;
		$this->connector = $connector;
		$this->host = $host;
		$this->resolvedHost = $resolvedHost;
		$this->user = $user;
		$this->pass = $pass;
		$this->db = $db;
		$this->ready = $ready;
		$this->connInfo = new ConnectionState;
	}

	private function ready() {
		if (empty($this->futures)) {
			if (empty($this->onReady)) {
				$cb = $this->ready;
			} else {
				list($key, $cb) = each($this->onReady);
				unset($this->onReady[$key]);
			}
			$cb();
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
		$this->inBuf = "";
		$this->inBuflen = 0;
		$this->out = [];

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
			$callback();
		}
	}

	public function getConnInfo() {
		return clone $this->connInfo;
	}

	private function startCommand($future = null) {
		$this->seqId = -1;
		return $this->futures[] = $future ?: new Future($this->reactor);
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
		$this->seqId = -1;

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
			$this->seqId = -1;
			$this->sendPacket($payload);
		});
	}

	/** @see 14.7.6 COM_STMT_EXECUTE */
	// @TODO what to do with the prebound params?! (bindParam())
	public function execute($stmtId, $data = []) {
		$payload = "\x17";
		$payload .= DataTypes::encode_int32($stmtId);
		$payload .= chr(0); // cursor flag // @TODO cursor types?!
		$payload .= DataTypes::encode_int32(1);
		$params = count($data);
		if ($params) {
			$nullOff = strlen($payload);
			$payload .= str_repeat("\0", ($params + 7) >> 3);
			$bound = 0;
			$types = "";
			$values = "";
			foreach ($data as $paramId => $param) {
				if ($param === null) {
					$off = $nullOff + ($paramId >> 3);
					$payload[$off] = $payload[$off] | chr(1 << ($paramId % 8));
				} else {
					$bound = 1;
				}
				list($unsigned, $type, $value) = DataTypes::encodeBinary($param);
				$types .= chr($type);
				$types .= $unsigned?"\x80":"\0";
				$values .= $value;
			}
			$payload .= chr($bound);
			if ($bound) {
				$payload .= $types;
				$payload .= $values;
			}
		}
		$future = new Future($this->reactor);
		$this->appendTask(function () use ($payload, $future) {
			$this->seqId = -1;
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
			$this->seqId = -1;
			$this->sendPacket($payload);
		});
	}

	public function resetStmt($stmtId) {
		$payload = "\x1a";
		$payload .= DataTypes::encode_int32($stmtId);
		$future = new Future($this->reactor);
		$this->appendTask(function () use ($payload, $future) {
			$this->seqId = -1;
			$this->futures[] = $future;
			$this->sendPacket($payload);
		});
		return $future;
	}

	public function onRead() {
		$this->inBuf .= $bytes = @fread($this->socket, $this->readGranularity);
		if ($bytes != "") {
			$len = strlen($bytes);

			$this->readLen += $len;
			$this->unreadlen = $this->inBuflen;
			$this->inBuflen += $len;
			try {
				if ($this->parse() === true) {
					//$this->handlePacket();
				}
			} catch (\Exception $e) {
				foreach ($this->futures as $future) {
					$future->fail($e);
				}
			}
		} else {
			// Gone away...
			// @TODO restart connection; throw error? remove from ready Connections
			var_dump("Gone away?!");
			$this->closeSocket();
		}
	}

	private function established() {
		// @TODO flags to use?
		$this->capabilities |= self::CLIENT_SESSION_TRACK | self::CLIENT_TRANSACTIONS | self::CLIENT_PROTOCOL_41 | self::CLIENT_SECURE_CONNECTION | self::CLIENT_MULTI_RESULTS | self::CLIENT_PS_MULTI_RESULTS | self::CLIENT_MULTI_STATEMENTS;

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
			$this->packetCallback = $this->parseCallback = null;
			if ($this->connectionState == self::READY) {
				// normal error
				$this->getFuture()->succeed(false);
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
			$this->state = ParseState::START;
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
		$this->resultSet = \Closure::bind(function &($prop, $val = NAN) { if (!@is_nan($val)) $this->prop = $val; return $this->$prop; }, $resultSet, $class);
		$this->resultSetMethod = \Closure::bind(function ($method, $args) { call_user_func_array([$this, $method], $args); }, $resultSet, $class);
	}

	/** @see 14.6.4.1.1 Text Resultset */
	private function handleQuery() {
		$this->getFuture()->succeed($resultSet = new ResultSet($this->reactor, $this->connInfo));
		$this->bindResultSet($resultSet);
		$this->parseCallback = [$this, "handleTextColumnDefinition"];
		$this->resultSetMethod("setColumns", ord($this->packet));
	}

	/** @see 14.7.1 Binary Protocol Resultset */
	private function handleExecute() {
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
				$this->parseCallback = [$this, $cbMethod];
				if ($this->capabilities & self::CLIENT_DEPRECATE_EOF) {
					$this->handleTextResultsetRow();
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
			$resultset = new Stmt($this->reactor, $this, $stmtId, $columns, $params);
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
		$this->connectionState = self::UNCONNECTED;
	}

	public function onWrite() {
		if ($this->outBuflen == 0) {
			$out = current($this->out);
			$len = strlen($out);
			$this->outBuf = substr_replace(pack("V", $len), chr(++$this->seqId), 3, 1) . $out; // expects $len < (1 << 24) - 1
			$this->outBuflen += 4 + $len;
		}

		if (defined("MYSQL_DEBUG")) {
			for ($i = 0; $i < $this->outBuflen; $i++)
				fwrite(STDERR, dechex(ord($this->outBuf[$i])) . " ");
			$r = range("\0", "\x19");
			unset($r[10], $r[9]);
			var_dump(str_replace($r, "", $this->outBuf));
		}

		$bytes = @fwrite($this->socket, $this->outBuf);
		$this->outBuflen -= $bytes;
		if ($this->outBuflen == 0) {
			unset($this->out[key($this->out)]);
			if (empty($this->out)) {
				$this->reactor->disable($this->writeWatcher);
				$this->watcherEnabled = false;
			}
		} else {
			// @TODO handle gone away
			$this->outBuf = substr($this->outBuf, $this->outBuflen);
		}
	}

	/**
	 * @see 14.1.2 MySQL Packet
	 * @see 14.1.3 Generic Response Packets
	 */
	private function parse() {
		start: {
			switch ($this->state) {
				case ParseState::START:
					goto determine_packet_len;
				case ParseState::PARSE_SEQ_ID:
					goto parse_seq_id;
				case ParseState::FETCH_PACKET:
					goto fetch_packet;
				case ParseState::DECODE_INT:
					goto decode_int;
				case ParseState::DECODE_INT8:
					goto decode_int8;
				case ParseState::DECODE_INT16:
					goto decode_int16;
				case ParseState::DECODE_INT24:
					goto decode_int24;
				case ParseState::DECODE_INT64:
					goto decode_int64;
				case ParseState::DECODE_STRING:
					goto decode_string_parse;
				case ParseState::DECODE_STRING_WAIT:
					goto decode_string_wait;
				case ParseState::DECODE_EOF_STRING:
					goto decode_eof_string_wait;
				case ParseState::DECODE_NULL_STRING:
					goto decode_null_string_wait;
				default:
					throw new \UnexpectedValueException("{$this->state} is not a valid ParseState constant");
			}
		}

		determine_packet_len: {
			if (isset($int)) {
				$this->packetSize = $int;
				unset($int);
				$this->state = ParseState::PARSE_SEQ_ID;
				goto parse_seq_id;
			} else {
				goto decode_int24;
			}
		}

		parse_seq_id: {
			if (isset($int)) {
				$this->seqId = $int;
				unset($int);
				$this->readLen -= 4;
				$this->state = ParseState::FETCH_PACKET;
				goto start;
			} else {
				goto decode_int8;
			}
		}

		fetch_packet: {
			if ($this->inBuflen < $this->packetSize) {
				goto more_data_needed;
			}

			if ($this->packetSize > 0) {
				$this->packet = substr($this->inBuf, 0, $this->packetSize);
				$this->inBuf = substr($this->inBuf, $this->packetSize);
				$this->inBuflen -= $this->packetSize;
				if (defined("MYSQL_DEBUG")) {
					$print = substr_replace(pack("V", $this->packetSize), chr($this->seqId), 3, 1);
					for ($i = 0; $i < 4; $i++)
						fwrite(STDERR, dechex(ord($print[$i])) . " ");
					for ($i = 0; $i < $this->packetSize; $i++)
						fwrite(STDERR, dechex(ord($this->packet[$i])) . " ");
					var_dump($this->packet);
				}
				if ($this->parseCallback) {
					$cb = $this->parseCallback;
					$cb();
					goto finished;
				} else {
					$this->packetType = ord($this->packet);
					goto payload;
				}
			}
		}

		payload: {
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
					if ($this->state !== self::READY) {
						/** @see 14.2.5 Connection Phase Packets (AuthMoreData) */
						// @TODO ... 14.2.2.2
						break;
					}
					/* intentionally missing break */
				default:
					if ($this->writeWatcher === NULL) {
						$this->established();
						$this->handleHandshake();
					} elseif ($this->packetCallback) {
						$cb = $this->packetCallback;
						$cb();
					} else {
						throw new \UnexpectedValueException("Unexpected packet type: {$this->packetType}");
					}
			}
			goto finished;
		}

		decode_int: {
			if ($this->inBuflen < 1) {
				goto more_data_needed;
			}

			$int = ord($this->inBuf);
			if ($int < 0xfb) {
				$this->inBuf = substr($this->inBuf, 1);
				$this->inBuflen -= 1;
				goto start;
			} elseif ($int == 0xfc) {
				goto decode_int16;
			} elseif ($int == 0xfd) {
				goto decode_int24;
			} elseif ($int == 0xfe) {
				goto decode_int64;
			} else {
				// If that happens connection is borked...
				throw new \RangeException("$int is not in ranges [0x00, 0xfa] or [0xfc, 0xfe]");
			}
		}

		decode_int8: {
			if ($this->inBuflen < 1) {
				goto more_data_needed;
			}

			$int = ord($this->inBuf);
			$this->inBuf = substr($this->inBuf, 1);
			$this->inBuflen -= 1;
			goto start;
		}

		decode_int16: {
			$intlen = isset($int) + 2;
			if ($this->inBuflen < $intlen) {
				goto more_data_needed;
			}

			$int = unpack("v", substr($this->inBuf, isset($int), 2))[1];
			$this->inBuf = substr($this->inBuf, $intlen);
			$this->inBuflen -= $intlen;
			goto start;
		}

		decode_int24: {
			$intlen = isset($int) + 3;
			if ($this->inBuflen < $intlen) {
				goto more_data_needed;
			}

			$int = unpack("V", substr($this->inBuf, isset($int), 3) . "\x00")[1];
			$this->inBuf = substr($this->inBuf, $intlen);
			$this->inBuflen -= $intlen;
			goto start;
		}

		decode_int32: {
			if ($this->inBuflen < 4) {
				goto more_data_needed;
			}

			$int = unpack("V", substr($this->inBuf, 0, 4))[1];
			$this->inBuf = substr($this->inBuf, 4);
			$this->inBuflen -= 4;
			goto start;
		}

		decode_int64: {
			$intlen = isset($int) + 8;
			if ($this->inBuflen < $intlen) {
				goto more_data_needed;
			}

			$int = unpack("V2", substr($this->inBuf, isset($int), 8));
			$int = $int[2] + ($int[1] << 32);
			$this->inBuf = substr($this->inBuf, $intlen);
			$this->inBuflen -= $intlen;
			goto start;
		}

		decode_eof_string: {
			$this->oldState = $this->state;
			$this->state = ParseState::DECODE_EOF_STRING;
			goto decode_eof_string_wait;
		}

		decode_eof_string_wait: {
			if ($this->readLen < $this->packetSize) {
				goto more_data_needed;
			}

			$this->strlen = $this->inBuflen - ($this->readLen - $this->packetSize);
			$string = substr($this->inBuf, 0, $this->strlen);
			$this->inBuf = substr($this->inBuf, $this->strlen);
			$this->inBuflen -= $this->strlen;
			$this->state = $this->oldState;
			goto start;
		}

		decode_string: {
			$this->oldState = $this->state;
			$this->state = ParseState::DECODE_STRING;
			goto decode_string_parse;
		}

		decode_string_parse: {
			if (isset($int)) {
				$this->strlen = $int;
				unset($int);
				$this->state = ParseState::DECODE_STRING_WAIT;
				goto decode_string_wait;
			} else {
				goto decode_int;
			}
		}

		decode_string_wait: {
			if ($this->inBuflen < $this->strlen) {
				goto more_data_needed;
			}

			$string = substr($this->inBuf, 0, $this->strlen);
			$this->inBuf = substr($this->inBuf, $this->strlen);
			$this->inBuflen -= $this->strlen;
			$this->state = $this->oldState;
			goto start;
		}

		decode_null_string: {
			$this->oldState = $this->state;
			$this->state = ParseState::DECODE_NULL_STRING;
			goto decode_null_string_wait;
		}

		decode_null_string_wait: {
			if (($this->strlen = strpos($this->inBuf, "\0", $this->strlen)) === false) {
				$this->strlen = $this->inBuflen;
				goto more_data_needed;
			}

			$this->inBuflen -= $this->strlen + 1;
			$string = substr($this->inBuf, 0, $this->strlen); // without NULL byte
			$this->inBuf = substr($this->inBuf, $this->strlen + 1);
			$this->state = $this->oldState;
			goto start;
		}

		finished: {
			$this->state = ParseState::START;
			if ($this->inBuflen > 0) {
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
		$payload .= chr($this->connInfo->charset); // @TODO: Use correct charset?!
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
	const DECODE_INT = 1;
	const DECODE_INT8 = 2;
	const DECODE_INT16 = 3;
	const DECODE_INT24 = 4;
	const DECODE_INT64 = 5;
	const DECODE_STRING = 6;
	const DECODE_STRING_WAIT = 7;
	const DECODE_NULL_STRING = 8;
	const DECODE_EOF_STRING = 9;
	const PARSE_SEQ_ID = 10;
	const FETCH_PACKET = 11;
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