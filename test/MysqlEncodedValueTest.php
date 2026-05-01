<?php declare(strict_types=1);

namespace Amp\Mysql\Test;

use Amp\Mysql\Internal\MysqlEncodedValue;
use Amp\Mysql\MysqlDataType;
use PHPUnit\Framework\TestCase;

class MysqlEncodedValueTest extends TestCase
{
    /**
     * @return iterable<string, array{MysqlDataType|null, MysqlDataType}>
     */
    public function provideStringTargets(): iterable
    {
        yield 'no target (legacy default)' => [null, MysqlDataType::VarString];
        yield 'varchar' => [MysqlDataType::Varchar, MysqlDataType::VarString];
        yield 'string' => [MysqlDataType::String, MysqlDataType::VarString];
        yield 'var_string' => [MysqlDataType::VarString, MysqlDataType::VarString];
        yield 'tiny_blob' => [MysqlDataType::TinyBlob, MysqlDataType::LongBlob];
        yield 'blob' => [MysqlDataType::Blob, MysqlDataType::LongBlob];
        yield 'medium_blob' => [MysqlDataType::MediumBlob, MysqlDataType::LongBlob];
        yield 'long_blob' => [MysqlDataType::LongBlob, MysqlDataType::LongBlob];
        yield 'bit' => [MysqlDataType::Bit, MysqlDataType::VarString];
        yield 'enum' => [MysqlDataType::Enum, MysqlDataType::VarString];
        yield 'set' => [MysqlDataType::Set, MysqlDataType::VarString];
        yield 'geometry' => [MysqlDataType::Geometry, MysqlDataType::VarString];
    }

    /**
     * @dataProvider provideStringTargets
     */
    public function testStringPicksTypeBasedOnTarget(?MysqlDataType $target, MysqlDataType $expected): void
    {
        $encoded = MysqlEncodedValue::fromValue('hello', $target);

        self::assertSame($expected, $encoded->getType());
    }

    public function testBinaryPayloadIntoBlobStaysLongBlob(): void
    {
        $encoded = MysqlEncodedValue::fromValue(\random_bytes(64), MysqlDataType::Blob);

        self::assertSame(MysqlDataType::LongBlob, $encoded->getType());
    }

    public function testStringableIntoBlobStaysLongBlob(): void
    {
        $stringable = new class implements \Stringable {
            public function __toString(): string
            {
                return 'cast me';
            }
        };

        $encoded = MysqlEncodedValue::fromValue($stringable, MysqlDataType::LongBlob);

        self::assertSame(MysqlDataType::LongBlob, $encoded->getType());
    }

    public function testBackedEnumIntoStringTargetUsesVarString(): void
    {
        $enum = StringTargetEnum::Foo;

        $encoded = MysqlEncodedValue::fromValue($enum, MysqlDataType::Varchar);

        self::assertSame(MysqlDataType::VarString, $encoded->getType());
    }

    public function testIntegerEncodingIgnoresTarget(): void
    {
        $encoded = MysqlEncodedValue::fromValue(1, MysqlDataType::LongBlob);

        self::assertSame(MysqlDataType::Tiny, $encoded->getType());
    }

    public function testNullEncodingIgnoresTarget(): void
    {
        $encoded = MysqlEncodedValue::fromValue(null, MysqlDataType::LongBlob);

        self::assertSame(MysqlDataType::Null, $encoded->getType());
    }
}

enum StringTargetEnum: string
{
    case Foo = 'foo';
}
