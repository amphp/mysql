<?php

namespace Amp\Mysql;

use Amp\Sql\SqlException;

/** @see 14.6.4.1.1.1 Column Type */
enum MysqlDataType: int
{
    case Decimal = 0x00;
    case Tiny = 0x01;
    case Short = 0x02;
    case Long = 0x03;
    case Float = 0x04;
    case Double = 0x05;
    case Null = 0x06;
    case Timestamp = 0x07;
    case LongLong = 0x08;
    case Int24 = 0x09;
    case Date = 0x0a;
    case Time = 0x0b;
    case Datetime = 0x0c;
    case Year = 0x0d;
    case NewDate = 0x0e;
    case Varchar = 0x0f;
    case Bit = 0x10;
    case Timestamp2 = 0x11;
    case Datetime2 = 0x12;
    case Time2 = 0x13;
    case Json = 0xf5;
    case NewDecimal = 0xf6;
    case Enum = 0xf7;
    case Set = 0xf8;
    case TinyBlob = 0xf9;
    case MediumBlob = 0xfa;
    case LongBlob = 0xfb;
    case Blob = 0xfc;
    case VarString = 0xfd;
    case String = 0xfe;
    case Geometry = 0xff;

    private const ENCODED_JSON_PREFIX = "base64:type251:";

    /**
     * @return array{self, string}
     *
     * @see 14.7.3 Binary Value
     */
    public static function encodeBinary(mixed $param): array
    {
        switch (\get_debug_type($param)) {
            case "bool":
                $encoded = $param ? "\x01" : "\0";
                return [self::Tiny, $encoded];

            case "int":
                if ($param >= -(1 << 15) && $param < (1 << 15)) {
                    return [self::Short, self::encodeInt16($param)];
                }

                if ($param >= -(1 << 31) && $param < (1 << 31)) {
                    return [self::Long, self::encodeInt32($param)];
                }

                return [self::LongLong, self::encodeInt64($param)];

            case "float":
                return [self::Double, \pack("e", $param)];

            case "string":
                return [self::LongBlob, self::encodeInt(\strlen($param)) . $param];

            case "null":
                return [self::Null, ""];

            default:
                throw new SqlException("Unexpected type for binding parameter: " . \get_debug_type($param));
        }
    }

    /** @see 14.7.3 Binary Protocol Value */
    public function decodeBinary(string $string, int &$offset = 0, int $flags = 0): int|float|string|null
    {
        $unsigned = $flags & 0x20;

        switch ($this) {
            case self::String:
            case self::Varchar:
            case self::VarString:
            case self::Enum:
            case self::Set:
            case self::LongBlob:
            case self::MediumBlob:
            case self::Blob:
            case self::TinyBlob:
            case self::Geometry:
            case self::Bit:
            case self::Decimal:
            case self::NewDecimal:
                return self::decodeString($string, $offset);

            case self::Json:
                $data = self::decodeString($string, $offset);
                return self::decodeJson($data);

            case self::LongLong:
                return $unsigned
                    ? self::decodeUnsigned64($string, $offset)
                    : self::decodeInt64($string, $offset);

            case self::Long:
                return $unsigned
                    ? self::decodeUnsigned32($string, $offset)
                    : self::decodeInt32($string, $offset);

            case self::Int24:
                return $unsigned
                    ? self::decodeUnsigned24($string, $offset)
                    : self::decodeInt24($string, $offset);

            case self::Short:
                return $unsigned
                    ? self::decodeUnsigned16($string, $offset)
                    : self::decodeInt16($string, $offset);

            case self::Tiny:
                return $unsigned
                    ? self::decodeUnsigned8($string, $offset)
                    : self::decodeInt8($string, $offset);

            case self::Double:
                $string = \substr($string, $offset, 8);
                $offset += 8;
                return \unpack("e", $string)[1];

            case self::Float:
                $string = \substr($string, $offset, 4);
                $offset += 4;
                return \unpack("g", $string)[1];

            case self::Date:
            case self::Datetime:
            case self::Timestamp:
                $string = \substr($string, $offset, 12);
                $year = $month = $day = $hour = $minute = $second = $microsecond = 0;
                switch ($length = \ord($string) + 1) {
                    case 12:
                        $microsecond = self::decodeUnsigned32(\substr($string, 8));
                        // no break
                    case 8:
                        $second = \ord($string[7]);
                        $minute = \ord($string[6]);
                        $hour = \ord($string[5]);
                        // no break
                    case 5:
                        $day = \ord($string[4]);
                        $month = \ord($string[3]);
                        $year = self::decodeUnsigned16(\substr($string, 1));
                        // no break
                    case 1:
                        break;

                    default:
                        throw new SqlException("Unexpected string length for date in binary protocol: " . ($length - 1));
                }

                return \str_pad((string) $year, 2, "0", \STR_PAD_LEFT)
                    . "-" . \str_pad((string) $month, 2, "0", \STR_PAD_LEFT)
                    . "-" . \str_pad((string) $day, 2, "0", \STR_PAD_LEFT)
                    . " " . \str_pad((string) $hour, 2, "0", \STR_PAD_LEFT)
                    . ":" . \str_pad((string) $minute, 2, "0", \STR_PAD_LEFT)
                    . ":" . \str_pad((string) $second, 2, "0", \STR_PAD_LEFT)
                    . "." . \str_pad((string) $microsecond, 5, "0", \STR_PAD_LEFT);

            case self::Time:
                $string = \substr($string, $offset, 13);
                $negative = $day = $hour = $minute = $second = $microsecond = 0;
                switch ($length = \ord($string) + 1) {
                    case 13:
                        $microsecond = self::decodeUnsigned32(\substr($string, 9));
                        // no break
                    case 9:
                        $second = \ord($string[8]);
                        $minute = \ord($string[7]);
                        $hour = \ord($string[6]);
                        $day = self::decodeUnsigned32(\substr($string, 2));
                        $negative = \ord($string[1]);
                        // no break
                    case 1:
                        break;

                    default:
                        throw new SqlException("Unexpected string length for time in binary protocol: " . ($length - 1));
                }

                return ($negative ? "" : "-") . \str_pad((string) $day, 2, "0", \STR_PAD_LEFT)
                    . "d " . \str_pad((string) $hour, 2, "0", \STR_PAD_LEFT)
                    . ":" . \str_pad((string) $minute, 2, "0", \STR_PAD_LEFT)
                    . ":" . \str_pad((string) $second, 2, "0", \STR_PAD_LEFT)
                    . "." . \str_pad((string) $microsecond, 5, "0", \STR_PAD_LEFT);

            case self::Null:
                return null;

            default:
                throw new SqlException("Invalid type for Binary Protocol: 0x" . \dechex($this->value));
        }
    }

    public function decodeText(string $string, int &$offset = 0, int $flags = 0): int|float|string
    {
        $length = self::decodeUnsigned($string, $offset);
        $offset += $length;
        $data = \substr($string, $offset - $length, $length);

        switch ($this) {
            case self::LongLong:
                if ($flags & 0x20) {
                    return $data; // Return UNSIGNED BIGINT as a string.
                }
                // no break

            case self::Long:
                if (\PHP_INT_SIZE < 8) {
                    return $data; // Return BIGINT and UNSIGNED INT as string on 32-bit.
                }
                // no break

            case self::Int24:
            case self::Short:
            case self::Tiny:
                return (int) $data;

            case self::Double:
            case self::Float:
                return (float) $data;

            case self::Json:
                return self::decodeJson($data);

            default:
                return $data;
        }
    }

    private static function decodeJson(string $data): string
    {
        if (\strncmp(self::ENCODED_JSON_PREFIX, $data, \strlen(self::ENCODED_JSON_PREFIX)) !== 0) {
            return $data; // Data was not base-64 encoded.
        }

        $data = \substr($data, \strlen(self::ENCODED_JSON_PREFIX));
        return \base64_decode($data);
    }

    public static function decodeNullTerminatedString(string $string, int &$offset = 0): string
    {
        $length = \strpos($string, "\0", $offset);
        if ($length === false) {
            throw new \ValueError('Null not found in string');
        }

        $length -= $offset;
        $result = \substr($string, $offset, $length);
        $offset += $length + 1;
        return $result;
    }

    public static function decodeString(string $string, int &$offset = 0): string
    {
        $length = self::decodeUnsigned($string, $offset);
        $offset += $length;
        return \substr($string, $offset - $length, $length);
    }

    public static function decodeUnsigned(string $string, int &$offset = 0): int
    {
        $int = self::decodeUnsigned8($string, $offset);
        if ($int < 0xfb) {
            return $int;
        }

        return match ($int) {
            0xfc => self::decodeUnsigned16($string, $offset),
            0xfd => self::decodeUnsigned24($string, $offset),
            0xfe => self::decodeUnsigned64($string, $offset),
            // If this happens connection is borked...
            default => throw new SqlException("$int is not in ranges [0x00, 0xfa] or [0xfc, 0xfe]"),
        };
    }

    public static function decodeIntByLength(string $string, int $length, int &$offset = 0): int
    {
        $int = 0;
        while ($length--) {
            $int = ($int << 8) + \ord($string[$length + $offset]);
        }

        $offset += $length;

        return $int;
    }

    public static function decodeInt8(string $string, int &$offset = 0): int
    {
        $int = \ord($string[$offset++]);
        if ($int < (1 << 7)) {
            return $int;
        }
        $shift = \PHP_INT_SIZE * 8 - 8;
        return $int << $shift >> $shift;
    }

    public static function decodeUnsigned8(string $string, int &$offset = 0): int
    {
        return \ord($string[$offset++]);
    }

    public static function decodeInt16(string $string, int &$offset = 0): int
    {
        $int = \unpack("v", \substr($string, $offset, 2))[1];
        $offset += 2;
        if ($int < (1 << 15)) {
            return $int;
        }
        $shift = \PHP_INT_SIZE * 8 - 16;
        return $int << $shift >> $shift;
    }

    public static function decodeUnsigned16(string $string, int &$offset = 0): int
    {
        $result = \unpack("v", \substr($string, $offset, 2))[1];
        $offset += 2;
        return $result;
    }

    public static function decodeInt24(string $string, int &$offset = 0): int
    {
        $int = \unpack("V", \substr($string, $offset, 3) . "\x00")[1];
        $offset += 3;
        if ($int < (1 << 23)) {
            return $int;
        }
        $shift = \PHP_INT_SIZE * 8 - 24;
        return $int << $shift >> $shift;
    }

    public static function decodeUnsigned24(string $string, int &$offset = 0): int
    {
        $result = \unpack("V", \substr($string, $offset, 3) . "\x00")[1];
        $offset += 3;
        return $result;
    }

    public static function decodeInt32(string $string, int &$offset = 0): int
    {
        $string = \substr($string, $offset, 4);
        $offset += 4;

        if (\PHP_INT_SIZE > 4) {
            $int = \unpack("V", $string)[1];
            if ($int < (1 << 31)) {
                return $int;
            }
            return $int << 32 >> 32;
        }

        return \unpack("V", $string)[1];
    }

    public static function decodeUnsigned32(string $string, int &$offset = 0): int
    {
        $string = \substr($string, $offset, 4);
        $offset += 4;

        $result = \unpack("V", $string)[1];
        if ($result < 0) {
            throw new \RuntimeException('Expecting a non-negative integer');
        }

        return $result;
    }

    public static function decodeUnsigned32WithGmp(string $string, int &$offset = 0): int|string
    {
        $string = \substr($string, $offset, 4);
        $offset += 4;

        if (\PHP_INT_SIZE > 4) {
            return \unpack("V", $string)[1];
        }

        \assert(\extension_loaded("gmp"), "The GMP extension is required for UNSIGNED INT fields on 32-bit systems");
        /** @psalm-suppress UndefinedConstant */
        return \gmp_strval(\gmp_import(\substr($string, 0, 4), 1, \GMP_LSW_FIRST));
    }

    public static function decodeInt64(string $string, int &$offset = 0): int
    {
        $string = \substr($string, $offset, 8);
        $offset += 8;

        if (\PHP_INT_SIZE > 4) {
            return \unpack("P", $string)[1];
        }

        throw new \RuntimeException('64-bit integers are not supported by 32-bit builds of PHP');
    }

    public static function decodeInt64WithGmp(string $string, int &$offset = 0): int|string
    {
        $string = \substr($string, $offset, 8);
        $offset += 8;

        if (\PHP_INT_SIZE > 4) {
            return \unpack("P", $string)[1];
        }

        \assert(\extension_loaded("gmp"), "The GMP extension is required for BIGINT fields on 32-bit systems");
        /** @psalm-suppress UndefinedConstant */
        return \gmp_strval(\gmp_import(\substr($string, 0, 8), 1, \GMP_LSW_FIRST));
    }

    public static function decodeUnsigned64(string $string, int &$offset = 0): int
    {
        if (\PHP_INT_SIZE <= 4) {
            throw new \RuntimeException('64-bit integers are not supported by 32-bit builds of PHP');
        }

        $string = \substr($string, $offset, 8);
        $offset += 8;

        $result = \unpack("P", $string)[1];
        if ($result < 0) {
            throw new \RuntimeException('Expecting a non-negative integer');
        }

        return $result;
    }

    public static function decodeUnsigned64WithGmp(string $string, int &$offset = 0): string
    {
        $string = \substr($string, $offset, 8);
        $offset += 8;

        \assert(\extension_loaded("gmp"), "The GMP extension is required for UNSIGNED BIGINT fields");
        /** @psalm-suppress UndefinedConstant */
        return \gmp_strval(\gmp_import(\substr($string, 0, 8), 1, \GMP_LSW_FIRST));
    }

    public static function encodeInt(int $int): string
    {
        if ($int < 0xfb) {
            return \chr($int);
        }

        if ($int < (1 << 16)) {
            return "\xfc" . self::encodeInt16($int);
        }

        if ($int < (1 << 24)) {
            return "\xfd" . self::encodeInt24($int);
        }

        return "\xfe" . self::encodeInt64($int);
    }

    public static function encodeInt16(int $int): string
    {
        return \pack("v", $int);
    }

    public static function encodeInt24(int $int): string
    {
        return \substr(\pack("V", $int), 0, 3);
    }

    public static function encodeInt32(int $int): string
    {
        return \pack("V", $int);
    }

    public static function encodeInt64(int $int): string
    {
        return \pack("VV", $int & 0xffffffff, $int >> 32);
    }
}
