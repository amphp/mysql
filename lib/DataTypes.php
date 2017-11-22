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
    const MYSQL_TYPE_JSON = 0xf5;
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

    /** @see 14.7.3 Binary Value */
    public static function encodeBinary($param): array {
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
                    $value = self::encodeInt16($param);
                    $type = self::MYSQL_TYPE_SHORT;
                } else {
                    $value = self::encodeInt64($param);
                    $type = self::MYSQL_TYPE_LONGLONG;
                }
                break;
            case "double":
                $value = pack("e", $param);
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
                throw new FailureException("Unexpected type for binding parameter: " . gettype($param));
        }

        return [$unsigned, $type, $value];
    }

    /** @see 14.7.3 Binary Protocol Value */
    public static function decodeBinary(int $type, string $str, /* ?int */ &$len = 0) {
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
            case self::MYSQL_TYPE_JSON:
            case self::MYSQL_TYPE_DECIMAL:
            case self::MYSQL_TYPE_NEWDECIMAL:
                $ret = self::decodeString($str, $intlen, $len);
                $len += $intlen;
                return $ret;

            case self::MYSQL_TYPE_LONGLONG:
            case self::MYSQL_TYPE_LONGLONG | 0x80:
                $len = 8;
                return $unsigned ? self::decodeUnsigned64($str) : self::decodeInt64($str);

            case self::MYSQL_TYPE_LONG:
            case self::MYSQL_TYPE_LONG | 0x80:
            case self::MYSQL_TYPE_INT24:
            case self::MYSQL_TYPE_INT24 | 0x80:
                $len = 4;
                return $unsigned ? self::decodeUnsigned32($str) : self::decodeInt32($str);

            case self::MYSQL_TYPE_SHORT:
            case self::MYSQL_TYPE_SHORT | 0x80:
                $len = 2;
                return $unsigned ? self::decodeUnsigned16($str) : self::decodeInt16($str);

            case self::MYSQL_TYPE_TINY:
            case self::MYSQL_TYPE_TINY | 0x80:
                $len = 1;
                return $unsigned ? \ord($str) : self::decodeInt8($str);

            case self::MYSQL_TYPE_DOUBLE:
                $len = 8;
                return unpack("e", $str)[1];

            case self::MYSQL_TYPE_FLOAT:
                $len = 4;
                return unpack("g", $str)[1];

            case self::MYSQL_TYPE_DATE:
            case self::MYSQL_TYPE_DATETIME:
            case self::MYSQL_TYPE_TIMESTAMP:
                $year = $month = $day = $hour = $minute = $second = $microsecond = 0;
                switch ($len = ord($str) + 1) {
                    case 12:
                        $microsecond = self::decodeUnsigned32(substr($str, 8));
                        // no break
                    case 8:
                        $second = ord($str[7]);
                        $minute = ord($str[6]);
                        $hour = ord($str[5]);
                        // no break
                    case 5:
                        $day = ord($str[4]);
                        $month = ord($str[3]);
                        $year = self::decodeUnsigned16(substr($str, 1));
                        // no break
                    case 1:
                        break;

                    default:
                        throw new FailureException("Unexpected string length for date in binary protocol: " . ($len - 1));
                }
                return str_pad($year, 2, "0", STR_PAD_LEFT) . "-" . str_pad($month, 2, "0", STR_PAD_LEFT) . "-" . str_pad($day, 2, "0", STR_PAD_LEFT) . " " . str_pad($hour, 2, "0", STR_PAD_LEFT) . ":" . str_pad($minute, 2, "0", STR_PAD_LEFT) . ":" . str_pad($second, 2, "0", STR_PAD_LEFT) . "." . str_pad($microsecond, 5, "0", STR_PAD_LEFT);

            case self::MYSQL_TYPE_TIME:
                $negative = $day = $hour = $minute = $second = $microsecond = 0;
                switch ($len = ord($str) + 1) {
                    case 13:
                        $microsecond = self::decodeUnsigned32(substr($str, 9));
                        // no break
                    case 9:
                        $second = ord($str[8]);
                        $minute = ord($str[7]);
                        $hour = ord($str[6]);
                        $day = self::decodeUnsigned32(substr($str, 2));
                        $negative = ord($str[1]);
                        // no break
                    case 1:
                        break;

                    default:
                        throw new FailureException("Unexpected string length for time in binary protocol: " . ($len - 1));
                }
                return ($negative ? "" : "-") . str_pad($day, 2, "0", STR_PAD_LEFT) . "d " . str_pad($hour, 2, "0", STR_PAD_LEFT) . ":" . str_pad($minute, 2, "0", STR_PAD_LEFT) . ":" . str_pad($second, 2, "0", STR_PAD_LEFT) . "." . str_pad($microsecond, 5, "0", STR_PAD_LEFT);

            case self::MYSQL_TYPE_NULL:
                $len = 0;
                return null;

            default:
                throw new FailureException("Invalid type for Binary Protocol: 0x" . dechex($type));
        }
    }

    public static function decodeNullString(string $str, /* ?int */ &$len = 0): string {
        return substr($str, 0, $len = strpos($str, "\0"));
    }

    public static function decodeStringOff(int $type, string $str, int &$off) {
        $len = self::decodeUnsignedOff($str, $off);
        $off += $len;
        $data = substr($str, $off - $len, $len);

        switch ($type) {
            case self::MYSQL_TYPE_LONGLONG:
            case self::MYSQL_TYPE_LONGLONG | 0x80:
            case self::MYSQL_TYPE_LONG:
            case self::MYSQL_TYPE_LONG | 0x80:
            case self::MYSQL_TYPE_INT24:
            case self::MYSQL_TYPE_INT24 | 0x80:
            case self::MYSQL_TYPE_SHORT:
            case self::MYSQL_TYPE_SHORT | 0x80:
            case self::MYSQL_TYPE_TINY:
            case self::MYSQL_TYPE_TINY | 0x80:
                return (int) $data;

            case self::MYSQL_TYPE_DOUBLE:
            case self::MYSQL_TYPE_FLOAT:
                return (float) $data;

            default:
                return $data;
        }
    }

    public static function decodeUnsignedOff(string $str, int &$off): int {
        $int = ord($str[$off]);
        if ($int < 0xfb) {
            $off += 1;
            return $int;
        }

        if ($int == 0xfc) {
            $off += 3;
            return self::decodeUnsigned16(substr($str, $off - 2, 2));
        }

        if ($int == 0xfd) {
            $off += 4;
            return self::decodeUnsigned24(substr($str, $off - 3, 3));
        }

        if ($int == 0xfe) {
            $off += 9;
            return self::decodeUnsigned64(substr($str, $off - 8, 8));
        }

        // If that happens connection is borked...
        throw new FailureException("$int is not in ranges [0x00, 0xfa] or [0xfc, 0xfe]");
    }

    public static function decodeString(string $str, /* ?int */ &$intlen = 0, /* ?int */ &$len = 0): string {
        $len = self::decodeUnsigned($str, $intlen);
        return substr($str, $intlen, $len);
    }

    public static function decodeUnsigned(string $str, /* ?int */ &$len = 0): int {
        $int = \ord($str);
        if ($int < 0xfb) {
            $len = 1;
            return $int;
        }

        if ($int == 0xfc) {
            $len = 3;
            return self::decodeUnsigned16(substr($str, 1, 2));
        }

        if ($int == 0xfd) {
            $len = 4;
            return self::decodeUnsigned24(substr($str, 1, 4));
        }

        if ($int == 0xfe) {
            $len = 9;
            return self::decodeUnsigned64(substr($str, 1, 8));
        }

        // If that happens connection is borked...
        throw new FailureException("$int is not in ranges [0x00, 0xfa] or [0xfc, 0xfe]");
    }

    public static function decodeIntByLen(string $str, int $len): int {
        $int = 0;
        while ($len--) {
            $int = ($int << 8) + ord($str[$len]);
        }
        return $int;
    }

    public static function decodeInt8(string $str): int {
        $int = \ord($str);
        if ($int < (1 << 7)) {
            return $int;
        }
        $shift = PHP_INT_SIZE * 8 - 8;
        return $int << $shift >> $shift;
    }

    public static function decodeUnsigned8(string $str): int {
        return \ord($str);
    }

    public static function decodeInt16(string $str): int {
        $int = unpack("v", $str)[1];
        if ($int < (1 << 15)) {
            return $int;
        }
        $shift = PHP_INT_SIZE * 8 - 16;
        return $int << $shift >> $shift;
    }

    public static function decodeUnsigned16(string $str): int {
        return unpack("v", $str)[1];
    }

    public static function decodeInt24(string $str): int {
        $int = unpack("V", substr($str, 0, 3) . "\x00")[1];
        if ($int < (1 << 23)) {
            return $int;
        }
        $shift = PHP_INT_SIZE * 8 - 24;
        return $int << $shift >> $shift;
    }

    public static function decodeUnsigned24(string $str): int {
        return unpack("V", substr($str, 0, 3) . "\x00")[1];
    }

    public static function decodeInt32($str) {
        if (PHP_INT_SIZE > 4) {
            $int = unpack("V", $str)[1];
            if ($int < (1 << 31)) {
                return $int;
            }
            return $int << 32 >> 32;
        }
        return unpack("V", $str)[1];
    }

    public static function decodeUnsigned32(string $str): int {
        if (PHP_INT_SIZE > 4) {
            return unpack("V", $str)[1];
        }
        $int = unpack("v", $str)[1];
        return $int[1] + ($int[2] * (1 << 16));
    }

    public static function decodeInt64(string $str): int {
        if (PHP_INT_SIZE > 4) {
            $int = unpack("V2", $str);
            return $int[1] + ($int[2] << 32);
        }

        $int = unpack("v2V", $str);
        return $int[1] + ($int[2] * (1 << 16)) + $int[3] * (1 << 16) * (1 << 16);
    }

    public static function decodeUnsigned64(string $str): int {
        if (PHP_INT_SIZE > 4) {
            $int = unpack("V2", $str);
            return $int[1] + $int[2] * (1 << 32);
        }

        $int = unpack("v4", $str);
        return $int[1] + ($int[2] * (1 << 16)) + ($int[3] + ($int[4] * (1 << 16))) * (1 << 16) * (1 << 16);
    }

    public static function encodeInt(int $int): string {
        if ($int < 0xfb) {
            return chr($int);
        }

        if ($int < (1 << 16)) {
            return "\xfc" . self::encodeInt16($int);
        }

        if ($int < (1 << 24)) {
            return "\xfd" . self::encodeInt24($int);
        }

        if ($int < (1 << 62) * 4) {
            return "\xfe" . self::encodeInt64($int);
        }

        throw new FailureException("encodeInt doesn't allow integers bigger than 2^64 - 1 (current: $int)");
    }

    public static function encodeInt16(int $int): string {
        return pack("v", $int);
    }

    public static function encodeInt24(int $int): string {
        return substr(pack("V", $int), 0, 3);
    }

    public static function encodeInt32(int $int): string {
        return pack("V", $int);
    }

    public static function encodeInt64(int $int): string {
        return pack("VV", $int & 0xffffffff, $int >> 32);
    }
}
