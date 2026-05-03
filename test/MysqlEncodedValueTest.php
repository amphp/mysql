<?php declare(strict_types=1);

namespace Amp\Mysql\Test;

use Amp\Mysql\Internal\MysqlEncodedValue;
use Amp\Mysql\MysqlDataType;
use PHPUnit\Framework\TestCase;

class MysqlEncodedValueTest extends TestCase
{
    public function testStringable(): void
    {
        $stringable = new class implements \Stringable {
            public function __toString(): string
            {
                return 'cast me';
            }
        };

        $encoded = MysqlEncodedValue::forTargetType(MysqlDataType::LongBlob, $stringable);

        self::assertSame(MysqlDataType::LongBlob, $encoded->getType());
    }

    public function testNullEncodingIgnoresTarget(): void
    {
        $encoded = MysqlEncodedValue::forTargetType(MysqlDataType::LongBlob, null);

        self::assertSame(MysqlDataType::Null, $encoded->getType());
    }

    public function testNonStringValueThrows(): void
    {
        self::expectException(\TypeError::class);

        MysqlEncodedValue::forTargetType(MysqlDataType::LongBlob, 1);
    }
}
