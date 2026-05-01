<?php declare(strict_types=1);

namespace Amp\Mysql\Internal;

use Amp\Mysql\MysqlDataType;

/** @internal */
final class MysqlEncodedValue
{
    public static function fromValue(mixed $param, ?MysqlDataType $targetType = null): self
    {
        switch (\get_debug_type($param)) {
            case "string":
                return new self(self::stringTypeFor($targetType), MysqlDataType::encodeInt(\strlen($param)) . $param);

            case "int":
                if ($param >= -(1 << 7) && $param < (1 << 7)) {
                    return new self(MysqlDataType::Tiny, MysqlDataType::encodeInt8($param));
                }

                if ($param >= -(1 << 15) && $param < (1 << 15)) {
                    return new self(MysqlDataType::Short, MysqlDataType::encodeInt16($param));
                }

                if ($param >= -(1 << 31) && $param < (1 << 31)) {
                    return new self(MysqlDataType::Long, MysqlDataType::encodeInt32($param));
                }

                return new self(MysqlDataType::LongLong, MysqlDataType::encodeInt64($param));

            case "float":
                return new self(MysqlDataType::Double, \pack("e", $param));

            case "bool":
                $encoded = $param ? "\x01" : "\0";
                return new self(MysqlDataType::Tiny, $encoded);

            case "null":
                return new self(MysqlDataType::Null, "");

            default:
                if ($param instanceof \BackedEnum) {
                    return self::fromValue($param->value, $targetType);
                }

                if ($param instanceof \Stringable) {
                    return self::fromValue((string) $param, $targetType);
                }

                throw new \TypeError("Unexpected type for query parameter: " . \get_debug_type($param));
        }
    }

    /**
     * Picks the wire type for a PHP string parameter. Blob-family targets keep
     * the binary (charset 63) interpretation of LongBlob so raw bytes are not
     * transcoded against the connection charset; everything else uses VarString
     * so MariaDB's native UUID column type and similar string-typed columns
     * parse the value correctly.
     */
    private static function stringTypeFor(?MysqlDataType $targetType): MysqlDataType
    {
        return match ($targetType) {
            MysqlDataType::TinyBlob,
            MysqlDataType::Blob,
            MysqlDataType::MediumBlob,
            MysqlDataType::LongBlob => MysqlDataType::LongBlob,
            default => MysqlDataType::VarString,
        };
    }

    public static function fromJson(?string $json): self
    {
        if ($json === null) {
            return new self(MysqlDataType::Null, "");
        }

        return new self(MysqlDataType::Json, MysqlDataType::encodeInt(\strlen($json)) . $json);
    }

    private function __construct(
        private readonly MysqlDataType $type,
        private readonly string $bytes,
    ) {
    }

    public function getType(): MysqlDataType
    {
        return $this->type;
    }

    public function getBytes(): string
    {
        return $this->bytes;
    }
}
