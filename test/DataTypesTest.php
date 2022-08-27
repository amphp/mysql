<?php

namespace Amp\Mysql\Test;

use Amp\Mysql\CancellableConnector;
use Amp\Mysql\ConnectionConfig;
use Amp\PHPUnit\AsyncTestCase;
use Amp\Promise;

class DataTypesTest extends AsyncTestCase
{
    protected function getLink(): Promise
    {
        return (new CancellableConnector())->connect(
            ConnectionConfig::fromString("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=test")
        );
    }

    public function provideDataAndTypes(): array
    {
        $now = new \DateTimeImmutable();

        return [
            // DATE
            ['1970-01-01', 'DATE'],
            ['2020-03-17', 'DATE'],
            [$now->format('Y-m-d'), 'DATE'],

            // DATETIME
            ['1970-01-01 00:00:00', 'DATETIME(6)'],
            [$now->format('Y-m-d H:i:s.u'), 'DATETIME(6)'],

            // TIME
            ['184:15:56.271282', 'TIME(6)'],
            [$now->format('H:i:s.u'), 'TIME(6)'],

            // YEAR
            [0, 'YEAR'],
            [1901, 'YEAR'],
            [2155, 'YEAR'],

            // FLOAT
            [3.1415926, 'FLOAT'],

            // DOUBLE
            [3.1415926, 'DOUBLE'],

            // SIGNED and UNSIGNED
            [-12345, 'SIGNED'],
            [12345, 'UNSIGNED'],
        ];
    }

    /**
     * @dataProvider provideDataAndTypes
     */
    public function testDataType($expected, string $type): \Generator
    {
        $db = yield $this->getLink();

        $result = yield $db->execute("SELECT CAST(:expected AS $type) AS data", ['expected' => $expected]);

        while (yield $result->advance()) {
            $data = $result->getCurrent()['data'];

            if (\is_float($expected)) {
                self::assertEqualsWithDelta($expected, $data, 1E-5);
            } else {
                self::assertSame($expected, $data);
            }
        }
    }

    public function provideJsonData(): \Generator
    {
        yield 'object' => ['
            {
                "object": {"key": "value"},
                "array": [1, 2, 3],
                "string": "value",
                "integer": 42,
                "float": 3.1415926,
                "boolean": true,
                "null": null
            }
            ', false];

        yield 'array' => ['[1, 2, 3]', true];

        yield 'float' => ['3.1415926', true];

        yield 'integer' => ['42', true];

        yield 'boolean' => ['true', true];

        yield 'null' => ['null', true];
    }

    /**
     * @dataProvider provideJsonData
     */
    public function testJson($json, bool $same)
    {
        $db = yield $this->getLink();

        $result = yield $db->execute("SELECT CAST(? AS JSON) AS data", [$json]);

        while (yield $result->advance()) {
            $expected = \json_decode($json);
            $actual = \json_decode($result->getCurrent()['data']);

            if ($same) {
                self::assertSame($expected, $actual);
            } else {
                self::assertEquals($expected, $actual);
            }
        }
    }
}
