<?php

namespace Mysql;
use Amp\Future;
use Amp\Reactor;
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
	private $readLen = 0;
	private $strlen;
	private $socket;
	private $readGranularity = 4096;
	private $readWatcher;
	private $writeWatcher = NULL;
	private $watcherEnabled = false;
	private $authPluginDataLen;
	private $parseCallback = null;

	private $reactor;
	private $connector;
	private $ready;
	private $futures = [];
	private $resultSet = null;
	private $resultSetMethod;
	private $host;
	private $resolvedHost;
	private $user;
	private $pass;
	private $db;
	private $oldDb = NULL;

	protected $affectedRows;
	protected $insertId;
	protected $statusFlags;
	protected $warnings;
	protected $statusInfo;
	protected $sessionState = [];
	protected $capabilities = 0;
	protected $errorMsg;
	protected $errorCode;
	protected $errorState; // begins with "#"
	protected $packetType;
	protected $serverVersion;
	protected $connectionId;
	protected $authPluginData;
	protected $serverCapabilities = 0;
	protected $charset;
	protected $authPluginName;

	protected $connectionState = self::UNCONNECTED;

	const MAX_PACKET_SIZE = 0xffffff;

	const CLIENT_LONG_FLAG = 0x00000004;
	const CLIENT_CONNECT_WITH_DB = 0x00000008;
	const CLIENT_PROTOCOL_41 = 0x00000200;
	const CLIENT_TRANSACTIONS = 0x00002000;
	const CLIENT_SECURE_CONNECTION = 0x00008000;
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
	}

	private function ready() {
		$cb = $this->ready;
		$cb();
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

	private function established() {
		// @TODO flags to use?
		$this->capabilities |= self::CLIENT_SESSION_TRACK | self::CLIENT_TRANSACTIONS | self::CLIENT_PROTOCOL_41 | self::CLIENT_SECURE_CONNECTION;

		$this->writeWatcher = $this->reactor->onWritable($this->socket, [$this, "onWrite"]);
		$this->reactor->disable($this->writeWatcher);
	}

	/** @return Future */
	private function getFuture() {
		list($key, $future) = each($this->futures);
		unset($this->futures[$key]);
		return $future;
	}

	/** @see 14.1.3.2 ERR-Packet */
	private function handleError() {
		$off = 1;

		err_packet: {
			$this->errorCode = $this->decode_int16(substr($this->packet, $off, 2));
			$off += 2;
			if ($this->capabilities & self::CLIENT_PROTOCOL_41) {
				// goto get_err_state;
			} else {
				goto fetch_err_msg;
			}
		}

		get_err_state: {
			$this->errorState = substr($this->packet, $off, 6);

			$off += 6;
			// goto fetch_err_msg;
		}

		fetch_err_msg: {
			$this->errorMsg = substr($this->packet, $off);

			// goto finished;
		}


		finished: {
			if ($this->connectionState == self::READY) {
				// normal error
				$this->ready();
				$this->getFuture()->succeed(false);
			} elseif ($this->connectionState == self::ESTABLISHED) {
				// connection failure
				$this->closeSocket();
				$this->getFuture()->fail(new \Exception("Could not connect to {$this->resolvedHost}: {$this->errorState} {$this->errorMsg}"));
			}
		}
	}

	/** @see 14.1.3.1 OK-Packet */
	private function parseOk() {
		$off = 1;

		ok_packet: {
			$this->affectedRows = $this->decodeInt(substr($this->packet, $off), $intlen);
			$off += $intlen;
			// goto get_last_insert_id;
		}

		get_last_insert_id: {
			$this->insertId = $this->decodeInt(substr($this->packet, $off), $intlen);
			$off += $intlen;
			if ($this->capabilities & (self::CLIENT_PROTOCOL_41 | self::CLIENT_TRANSACTIONS)) {
				// goto get_status_flags;
			} else {
				goto fetch_status_info;
			}
		}

		get_status_flags: {
			$this->statusFlags = $this->decode_int16(substr($this->packet, $off));
			if ($this->statusFlags & StatusFlags::SERVER_MORE_RESULTS_EXISTS) {
				$this->parseCallback = [$this, "handleQuery"];
			}
			$off += 2;
			// goto get_warning_count;
		}

		get_warning_count: {
			$this->warnings = $this->decode_int16(substr($this->packet, $off));
			$off += 2;
			// goto fetch_status_info;
		}

		fetch_status_info: {
			if ($this->capabilities & self::CLIENT_SESSION_TRACK) {
				$this->statusInfo = $this->decodeString(substr($this->packet, $off), $intlen, $strlen);
				$off += $intlen + $strlen;
				if ($this->statusFlags & StatusFlags::SERVER_SESSION_STATE_CHANGED) {
					goto fetch_state_changes;
				}
			} else {
				$this->statusInfo = substr($this->packet, $off);
			}
			goto finished;
		}

		fetch_state_changes: {
			$sessionState = $this->decodeString(substr($this->packet, $off), $intlen, $sessionStateLen);
			$len = 0;
			while ($len < $sessionStateLen) {
				$data = $this->decodeString(substr($sessionState, $len + 1), $datalen);

				switch ($type = $this->decode_int8(substr($sessionState, $len))) {
					case SessionStateTypes::SESSION_TRACK_SYSTEM_VARIABLES:
						$this->sessionState[SessionStateTypes::SESSION_TRACK_SYSTEM_VARIABLES][$this->decodeString($data, $intlen, $strlen)] = $this->decodeString(substr($data, $intlen + $strlen));
						break;
					case SessionStateTypes::SESSION_TRACK_SCHEMA:
						$this->sessionState[SessionStateTypes::SESSION_TRACK_SCHEMA] = $this->decodeString($data);
						break;
					case SessionStateTypes::SESSION_TRACK_STATE_CHANGE:
						$this->sessionState[SessionStateTypes::SESSION_TRACK_STATE_CHANGE] = $this->decodeString($data);
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
		$this->ready();
		$this->getFuture()->succeed(true);
	}

	/** @see 14.1.3.3 EOF-Packet */
	private function parseEof() {
		$off = 1;

		eof_packet: {
			if ($this->capabilities & self::CLIENT_PROTOCOL_41) {
				$this->warnings = $this->decode_int16(substr($this->packet, $off));
				$off += 2;
				// goto get_eof_status_flags;
			} else {
				goto finished;
			}
		}

		get_eof_status_flags: {
			$this->statusFlags = $this->decode_int16(substr($this->packet, $off));
			if ($this->statusFlags & StatusFlags::SERVER_MORE_RESULTS_EXISTS) {
				$this->parseCallback = [$this, "handleQuery"];
			}
			// goto finished;
		}

		finished: {
			return;
		}
	}

	private function handleEof() {
		$this->parseEof();
		$this->ready();
		$this->getFuture()->succeed(true);
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
			$this->serverVersion = $this->decodeNullString(substr($this->packet, $off), $len);
			$off += $len + 1;
			// goto get_connection_id;
		}

		get_connection_id: {
			$this->connectionId = $this->decode_int32(substr($this->packet, $off));
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
			$this->serverCapabilities = $this->decode_int16(substr($this->packet, $off));
			$off += 2;
			if ($this->packetSize > $off) {
				// goto charset;
			} else {
				goto do_handshake;
			}
		}

		charset: {
			$this->charset = ord(substr($this->packet, $off));
			$off += 1;
			// goto handshake_status_flags;
		}

		handshake_status_flags: {
			$this->statusFlags = $this->decode_int16(substr($this->packet, $off));
			$off += 2;
			// goto read_capability_flags2;
		}

		read_capability_flags2: {
			$this->serverCapabilities += $this->decode_int16(substr($this->packet, $off)) << 32;
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
			$this->authPluginName = $this->decodeNullString(substr($this->packet, $off));
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

	private function &resultSetMethod($method) {
		$args = func_get_args();
		unset($args[0]);
		$cb = $this->resultSetMethod;
		return $cb($method, $args);
	}

	/** @see 14.6.4.1 COM_QUERY Response */
	private function handleQuery() {
		$this->getFuture()->succeed($resultSet = new ResultSet($this->reactor, ord($this->packet)));
		$this->resultSet = \Closure::bind(function &($prop, $val = NAN) { if (!is_nan($val)) $this->prop = $val; return $this->$prop; }, $resultSet, ResultSet::class);
		$this->resultSetMethod = \Closure::bind(function &($method, $args) { call_user_func_array([$this, $method], $args); }, $resultSet, ResultSet::class);
		$this->parseCallback = [$this, "handleColumnDefinition"];
	}

	/** @see 14.6.4.1.1.2 Column Defintion */
	private function handleColumnDefinition() {
		$toFetch = &$this->resultSet("columnsToFetch");
		if (!$toFetch--) {
			$this->resultSetMethod("updateState", ResultSet::COLUMNS_FETCHED);
			if (ord($this->packet) == 0xff) {
				$this->parseCallback = null;
				$this->handleError();
			} else {
				$this->parseCallback = [$this, "handleResultsetRow"];
				if ($this->capabilities & self::CLIENT_DEPRECATE_EOF) {
					$this->handleResultsetRow();
				} else {
					// we don't need the EOF packet, skip!
				}
			}
			return;
		}

		$off = 0;

		$column = [];

		if ($this->capabilities & self::CLIENT_PROTOCOL_41) {
			get_catalog: {
				$column["catalog"] = $this->decodeString(substr($this->packet, $off), $intlen, $len);
				$off += $intlen + $len;
				// goto get_schema;
			}

			get_schema: {
				$column["schema"] = $this->decodeString(substr($this->packet, $off), $intlen, $len);
				$off += $intlen + $len;
				// goto get_table_41;
			}

			get_table_41: {
				$column["table"] = $this->decodeString(substr($this->packet, $off), $intlen, $len);
				$off += $intlen + $len;
				// goto get_original_table;
			}

			get_original_table: {
				$column["original_table"] = $this->decodeString(substr($this->packet, $off), $intlen, $len);
				$off += $intlen + $len;
				// goto get_name_41;
			}

			get_name_41: {
				$column["name"] = $this->decodeString(substr($this->packet, $off), $intlen, $len);
				$off += $intlen + $len;
				// goto get_original_name;
			}

			get_original_name: {
				$column["original_name"] = $this->decodeString(substr($this->packet, $off), $intlen, $len);
				$off += $intlen + $len;
				// goto get_fixlen_len;
			}

			get_fixlen_len: {
				$fixlen = $this->decodeInt(substr($this->packet, $off), $len);
				$off += $len;
				// goto get_fixlen;
			}

			get_fixlen: {
				$len = 0;
				$column["charset"] = $this->decode_int16(substr($this->packet, $off + $len));
				$len += 2;
				$column["columnlen"] = $this->decode_int32(substr($this->packet, $off + $len));
				$len += 4;
				$column["type"] = ord($this->packet[$off + $len]);
				$len += 1;
				$column["flags"] = $this->decode_int16(substr($this->packet, $off + $len));
				$len += 2;
				$column["decimals"] = ord($this->packet[$off + $len]);
				$len += 1;

				$off += $len;
				// goto field_fetch;
			}
		} else {
			get_table_320: {
				$column["table"] = $this->decodeString(substr($this->packet, $off), $intlen, $len);
				$off += $intlen + $len;
				// goto get_name_320;
			}

			get_name_320: {
				$column["name"] = $this->decodeString(substr($this->packet, $off), $intlen, $len);
				$off += $intlen + $len;
				// goto get_columnlen_len;
			}

			get_columnlen_len: {
				$collen = $this->decodeInt(substr($this->packet, $off), $len);
				$off += $len;
				// goto get_columnlen;
			}

			get_columnlen: {
				$column["columnlen"] = $this->decode_intByLen(substr($this->packet, $off), $collen);
				$off += $collen;
				// goto type_len;
			}

			get_type_len: {
				$typelen = $this->decodeInt(substr($this->packet, $off), $len);
				$off += $len;
				// goto get_type;
			}

			get_type: {
				$column["type"] = $this->decode_intByLen(substr($this->packet, $off), $typelen);
				$off += $typelen;
				// goto get_flaglen;
			}

			get_flaglen: {
				$len = 1;
				$flaglen = $this->capabilities & self::CLIENT_LONG_FLAG ? $this->decodeInt(substr($this->packet, $off), $len) : ord($this->packet[$off]);
				$off += $len;
				// goto get_flags;
			}

			get_flags: {
				if ($flaglen > 2) {
					$len = 2;
					$column["flags"] = $this->decode_int16(substr($this->packet, $off));
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
				$column["defaults"] = $this->decodeString(substr($this->packet, $off));
			}
			// goto finished;
		}

		finished: {
			$this->resultSet("columns")[] = $column;
		}
	}

	/** @see 14.6.4.1.1.3 Resultset Row */
	private function handleResultsetRow() {
		switch ($type = ord($this->packet)) {
			case self::OK_PACKET:
				$this->parseOk();
				/* intentional fall through */
			case self::EOF_PACKET:
				if ($type == self::EOF_PACKET) {
					$this->parseEof();
				}
				$this->parseCallback = null;
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
				$fields[] = $this->decodeString(substr($this->packet, $off), $intlen, $len);
				$off += $intlen + $len;
			}
		}
		$this->resultSetMethod("rowFetched", $fields);
	}

	/** @see 14.6.2 COM_QUIT */
	public function closeConnection($future) {
		$this->sendPacket("\x01");
		$this->connectionState = self::QUITTING;
		$this->futures[] = $future;
	}

	/** @see 14.6.3 COM_INIT_DB */
	public function useDb($future, $db) {
		$this->oldDb = $this->db;
		$this->db = $db;
		$this->sendPacket("\x02$db");
		$this->futures[] = $future;
	}

	/** @see 14.6.4 COM_QUERY */
	public function query($future, $query) {
		$this->seqId = -1;
		$this->sendPacket("\x03$query");
		$this->futures[] = $future;
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

		for ($i = 0; $i < $this->outBuflen; $i++) fwrite(STDERR, dechex(ord($this->outBuf[$i]))." ");
		var_dump($this->outBuf);

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
				//var_dump($this->inBuf);
				$this->inBuf = substr($this->inBuf, $this->packetSize);
				$this->inBuflen -= $this->packetSize;
				for ($i = 0; $i < $this->packetSize; $i++) fwrite(STDERR, dechex(ord($this->packet[$i]))." ");
				var_dump($this->packet);
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
					} else {
						$this->handleQuery();
//						throw new \UnexpectedValueException("Unexpected packet type: {$this->packetType}");
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
			unset($this->reactor, $this->futures, $this->connector);
			var_dump($this);
			die();
			return NULL;
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
		$payload .= chr($this->charset); // @TODO: Use correct charset?!
		$payload .= str_repeat("\0", 23); // reserved
		$payload .= $this->user."\0";
		if ($this->capabilities & self::CLIENT_PLUGIN_AUTH) {
			$auth = ""; // @TODO AUTH
		} elseif ($this->pass !== "") {
			$hash = sha1($this->pass, 1);
			$auth = $hash ^ sha1(substr($this->authPluginData, 0, 20) . sha1($hash, 1), 1);
		} else {
			$auth = "";
		}
		if ($this->capabilities & self::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) {
			$payload .= $this->encodeInt(strlen($auth));
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

	private function decodeNullString($str, &$len = 0) {
		return substr($str, 0, $len = strpos($str, "\0"));
	}

	private function decodeString($str, &$intlen = 0, &$len = 0) {
		$len = $this->decodeInt($str, $intlen);
		return substr($str, $intlen, $len);
	}

	private function decodeInt($str, &$len = 0) {
		$int = ord($str);
		if ($int < 0xfb) {
			$len = 1;
			return $int;
		} elseif ($int == 0xfc) {
			$len = 3;
			return $this->decode_int16(substr($str, 1));
		} elseif ($int == 0xfd) {
			$len = 4;
			return $this->decode_int24(substr($str, 1));
		} elseif ($int == 0xfe) {
			$len = 9;
			return $this->decode_int64(substr($str, 1));
		} else {
			// If that happens connection is borked...
			throw new \RangeException("$int is not in ranges [0x00, 0xfa] or [0xfc, 0xfe]");
		}
	}

	private function decode_intByLen($str, $len) {
		$int = 0;
		while ($len--) {
			$int = ($int << 8) + ord($str[$len]);
		}
		return $int;
	}

	private function decode_int8($str) {
		return ord($str);
	}

	private function decode_int16($str) {
		return unpack("v", $str)[1];
	}

	private function decode_int24($str) {
		return unpack("V", substr($str, isset($int), 3) . "\x00")[1];
	}

	private function decode_int32($str) {
		return unpack("V", $str)[1];
	}

	private function decode_int64($str) {
		$int = unpack("V2", substr($str, isset($int), 8));
		return $int[2] + ($int[1] << 32);
	}

	private function encodeInt($int) {
		if ($int < 0xfb) {
			return chr($int);
		} elseif ($int < (1 << 16)) {
			return "\xfc".$this->encode_int16($int);
		} elseif ($int < (1 << 24)) {
			return "\xfd".$this->encode_int24($int);
		} elseif ($int < (1 << 62) * 4) {
			return "\xfe".$this->encode_int64($int);
		} else {
			throw new \OutOfRangeException("encodeInt doesn't allow integers bigger than 2^64 - 1 (current: $int)");
		}
	}

	private function encode_int16($int) {
		return pack("v", $int);
	}

	private function encode_int24($int) {
		return substr(pack("V", $int), 0, 3);
	}

	private function encode_int64($int) {
		return pack("VV", $int & 0xfffffff, $int >> 32);
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