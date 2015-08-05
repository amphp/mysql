<?php

namespace Amp\Mysql;

/** @see 14.6.4.1.1.1 Column Type */
class DataTypes {
	const MYSQL_TYPE_DECIMAL = 0x00;
	const MYSQL_TYPE_TINY = 0x01;
	const MYSQL_TYPE_SHORT = 0x02;
	const MYSQL_TYPE_LONG = 0x03;
	const MYSQL_TYPE_FLOAT = 0x04;
	const MYSQL_TYPE_DOUBLE = 0x05;
	const MYSQL_TYPE_NULL = 0x06;
	const MYSQL_TYPE_TIMESTAMP = 0x07;
	const MYSQL_TYPE_LONGLONG = 0x08;
	const MYSQL_TYPE_INT24 = 0x09;
	const MYSQL_TYPE_DATE = 0x0a;
	const MYSQL_TYPE_TIME = 0x0b;
	const MYSQL_TYPE_DATETIME = 0x0c;
	const MYSQL_TYPE_YEAR = 0x0d;
	const MYSQL_TYPE_NEWDATE = 0x0e;
	const MYSQL_TYPE_VARCHAR = 0x0f;
	const MYSQL_TYPE_BIT = 0x10;
	const MYSQL_TYPE_TIMESTAMP2 = 0x11;
	const MYSQL_TYPE_DATETIME2 = 0x12;
	const MYSQL_TYPE_TIME2 = 0x13;
	const MYSQL_TYPE_NEWDECIMAL = 0xf6;
	const MYSQL_TYPE_ENUM = 0xf7;
	const MYSQL_TYPE_SET = 0xf8;
	const MYSQL_TYPE_TINY_BLOB = 0xf9;
	const MYSQL_TYPE_MEDIUM_BLOB = 0xfa;
	const MYSQL_TYPE_LONG_BLOB = 0xfb;
	const MYSQL_TYPE_BLOB = 0xfc;
	const MYSQL_TYPE_VAR_STRING = 0xfd;
	const MYSQL_TYPE_STRING = 0xfe;
	const MYSQL_TYPE_GEOMETRY = 0xff;

	private static function isLittleEndian() {
		static $result = null;
		if ($result === null) {
			return $result = unpack('S', "\x01\x00")[1] === 1;
		}
		return $result;
	}


	/** @see 14.7.3 Binary Value */
	public static function encodeBinary($param) {
		$unsigned = 0;

		switch (gettype($param)) {
			case "boolean":
				$type = self::MYSQL_TYPE_TINY;
				$value = $param ? "\x01" : "\0";
				break;
			case "integer":
				if ($param >= 0) {
					$unsigned = 1;
				}
				if ($param >= 0 && $param < (1 << 15)) {
					$value = self::encode_int16($param);
					$type = self::MYSQL_TYPE_SHORT;
				} else {
					$value = self::encode_int64($param);
					$type = self::MYSQL_TYPE_LONGLONG;
				}
				break;
			case "double":
				$value = pack("d", $param);
				if (self::isLittleEndian()) {
					$value = strrev($value);
				}
				$type = self::MYSQL_TYPE_DOUBLE;
				break;
			case "string":
				$type = self::MYSQL_TYPE_LONG_BLOB;
				$value = self::encodeInt(strlen($param)) . $param;
				break;
			case "NULL":
				$type = self::MYSQL_TYPE_NULL;
				$value = "";
				break;
			default:
				throw new \UnexpectedValueException("Unexpected type for binding parameter: " . gettype($param));
		}

		return [$unsigned, $type, $value];
	}

	/** @see 14.7.3 Binary Protocol Value */
	public static function decodeBinary($type, $str, &$len = 0) {
		$unsigned = $type & 0x80;
		switch ($type) {
			case self::MYSQL_TYPE_STRING:
			case self::MYSQL_TYPE_VARCHAR:
			case self::MYSQL_TYPE_VAR_STRING:
			case self::MYSQL_TYPE_ENUM:
			case self::MYSQL_TYPE_SET:
			case self::MYSQL_TYPE_LONG_BLOB:
			case self::MYSQL_TYPE_MEDIUM_BLOB:
			case self::MYSQL_TYPE_BLOB:
			case self::MYSQL_TYPE_TINY_BLOB:
			case self::MYSQL_TYPE_GEOMETRY:
			case self::MYSQL_TYPE_BIT:
			case self::MYSQL_TYPE_DECIMAL:
			case self::MYSQL_TYPE_NEWDECIMAL:
				$ret = self::decodeString($str, $intlen, $len);
				$len += $intlen;
				return $ret;

			case self::MYSQL_TYPE_LONGLONG:
			case self::MYSQL_TYPE_LONGLONG | 0x80:
				$len = 8;
				return $unsigned && ($str[7] & "\x80") ? self::decode_unsigned64($str) : self::decode_int64($str);

			case self::MYSQL_TYPE_LONG:
			case self::MYSQL_TYPE_LONG | 0x80:
			case self::MYSQL_TYPE_INT24:
			case self::MYSQL_TYPE_INT24 | 0x80:
				$len = 4;
				$shift = PHP_INT_MAX >> 63 ? 32 : 0;
				return $unsigned && ($str[3] & "\x80") ? self::decode_unsigned32($str) : ((self::decode_int32($str) << $shift) >> $shift);

			case self::MYSQL_TYPE_TINY:
			case self::MYSQL_TYPE_TINY | 0x80:
				$len = 1;
				$shift = PHP_INT_MAX >> 63 ? 56 : 24;
				return $unsigned ? ord($str) : ((ord($str) << $shift) >> $shift);

			case self::MYSQL_TYPE_DOUBLE:
				$len = 8;
				return unpack("d", $str)[1];

			case self::MYSQL_TYPE_FLOAT:
				$len = 4;
				return unpack("f", $str)[1];

			case self::MYSQL_TYPE_DATE:
			case self::MYSQL_TYPE_DATETIME:
			case self::MYSQL_TYPE_TIMESTAMP:
				$year = $month = $day = $hour = $minute = $second = $microsecond = 0;
				switch ($len = ord($str) + 1) {
					case 12:
						$microsecond = self::decode_int32(substr($str, 8));
					case 8:
						$second = ord($str[7]);
						$minute = ord($str[6]);
						$hour = ord($str[5]);
					case 5:
						$day = ord($str[4]);
						$month = ord($str[3]);
						$year = self::decode_int16(substr($str, 1));
					case 1:
						break;

					default:
						throw new \UnexpectedValueException("Unexpected string length for date in binary protocol: " . ($len - 1));
				}
				return str_pad($year, 2, "0", STR_PAD_LEFT) . "-" . str_pad($month, 2, "0", STR_PAD_LEFT) . "-" . str_pad($day, 2, "0", STR_PAD_LEFT) . " " . str_pad($hour, 2, "0", STR_PAD_LEFT) . ":" . str_pad($minute, 2, "0", STR_PAD_LEFT) . ":" . str_pad($second, 2, "0", STR_PAD_LEFT) . "." . str_pad($microsecond, 5, "0", STR_PAD_LEFT);

			case self::MYSQL_TYPE_TIME:
				$negative = $day = $hour = $minute = $second = $microsecond = 0;
				switch ($len = ord($str) + 1) {
					case 13:
						$microsecond = self::decode_int32(substr($str, 9));
					case 9:
						$second = ord($str[8]);
						$minute = ord($str[7]);
						$hour = ord($str[6]);
						$day = self::decode_int32(substr($str, 2));
						$negative = ord($str[1]);
					case 1:
						break;

					default:
						throw new \UnexpectedValueException("Unexpected string length for time in binary protocol: " . ($len - 1));
				}
				return ($negative ? "" : "-") . str_pad($day, 2, "0", STR_PAD_LEFT) . "d " . str_pad($hour, 2, "0", STR_PAD_LEFT) . ":" . str_pad($minute, 2, "0", STR_PAD_LEFT) . ":" . str_pad($second, 2, "0", STR_PAD_LEFT) . "." . str_pad($microsecond, 5, "0", STR_PAD_LEFT);

			case self::MYSQL_TYPE_NULL:
				$len = 0;
				return null;

			default:
				throw new \UnexpectedValueException("Invalid type for Binary Protocol: 0x" . dechex($type));
		}
	}

	public static function decodeNullString($str, &$len = 0) {
		return substr($str, 0, $len = strpos($str, "\0"));
	}

	public static function decodeString($str, &$intlen = 0, &$len = 0) {
		$len = self::decodeInt($str, $intlen);
		return substr($str, $intlen, $len);
	}

	public static function decodeInt($str, &$len = 0) {
		$int = ord($str);
		if ($int < 0xfb) {
			$len = 1;
			return $int;
		} elseif ($int == 0xfc) {
			$len = 3;
			return self::decode_int16(substr($str, 1));
		} elseif ($int == 0xfd) {
			$len = 4;
			return self::decode_int24(substr($str, 1));
		} elseif ($int == 0xfe) {
			$len = 9;
			return self::decode_int64(substr($str, 1));
		} else {
			// If that happens connection is borked...
			throw new \RangeException("$int is not in ranges [0x00, 0xfa] or [0xfc, 0xfe]");
		}
	}

	public static function decode_intByLen($str, $len) {
		$int = 0;
		while ($len--) {
			$int = ($int << 8) + ord($str[$len]);
		}
		return $int;
	}

	public static function decode_int8($str) {
		return ord($str);
	}

	public static function decode_int16($str) {
		return unpack("v", $str)[1];
	}

	public static function decode_int24($str) {
		return unpack("V", substr($str, 0, 3) . "\x00")[1];
	}

	public static function decode_int32($str) {
		return unpack("V", $str)[1];
	}

	public static function decode_unsigned32($str) {
		if (PHP_INT_MAX >> 31) {
			return unpack("V", $str)[1];
		} else {
			$int = unpack("v", $str)[1];
			return $int[1] + ($int[2] * (1 << 16));
		}
	}

	public static function decode_int64($str) {
		if (PHP_INT_MAX >> 31) {
			$int = unpack("V2", $str);
			return $int[1] + ($int[2] << 32);
		} else {
			$int = unpack("v2V", $str);
			return $int[1] + ($int[2] * (1 << 16)) + $int[3] * (1 << 16) * (1 << 16);
		}
	}

	public static function decode_unsigned64($str) {
		if (PHP_INT_MAX >> 31) {
			$int = unpack("V2", $str);
			return $int[1] + $int[2] * (1 << 32);
		} else {
			$int = unpack("v4", $str);
			return $int[1] + ($int[2] * (1 << 16)) + ($int[3] + ($int[4] * (1 << 16))) * (1 << 16) * (1 << 16);
		}
	}

	public static function encodeInt($int) {
		if ($int < 0xfb) {
			return chr($int);
		} elseif ($int < (1 << 16)) {
			return "\xfc" . self::encode_int16($int);
		} elseif ($int < (1 << 24)) {
			return "\xfd" . self::encode_int24($int);
		} elseif ($int < (1 << 62) * 4) {
			return "\xfe" . self::encode_int64($int);
		} else {
			throw new \OutOfRangeException("encodeInt doesn't allow integers bigger than 2^64 - 1 (current: $int)");
		}
	}

	public static function encode_int16($int) {
		return pack("v", $int);
	}

	public static function encode_int24($int) {
		return substr(pack("V", $int), 0, 3);
	}

	public static function encode_int32($int) {
		return pack("V", $int);
	}

	public static function encode_int64($int) {
		return pack("VV", $int & 0xffffffff, $int >> 32);
	}
}