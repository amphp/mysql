<?php

namespace Amp\Mysql\Test;

use Amp\Mysql\MysqlConnection;
use Amp\Mysql\SocketMysqlConnector;

class MysqlDataTypeTest extends MysqlTestCase
{
    private readonly MysqlConnection $connection;

    public function setUp(): void
    {
        parent::setUp();

        $this->connection = (new SocketMysqlConnector)->connect($this->getConfig());
    }

    public function provideDataAndTypes(): array
    {
        $now = new \DateTimeImmutable();

        return [
            // DATE
            ['0000-00-00', 'DATE'],
            ['1970-01-01', 'DATE'],
            ['2020-03-17', 'DATE'],
            [$now->format('Y-m-d'), 'DATE'],

            // DATETIME
            ['0000-00-00 00:00:00', 'DATETIME(6)'],
            ['1970-01-01 00:00:00', 'DATETIME(6)'],
            [$now->format('Y-m-d H:i:s.u'), 'DATETIME(6)'],

            // TIME
            ['00:00:00', 'TIME(6)'],
            ['96:42:23', 'TIME(6)'],
            ['184:15:56.271282', 'TIME(6)'],
            [$now->format('H:i:s.u'), 'TIME(6)'],

            // YEAR
            [0, 'YEAR'],
            [1901, 'YEAR'],
            [2155, 'YEAR'],

            // FLOAT
            [0.0, 'FLOAT'],
            [\M_PI, 'FLOAT'],
            [-\M_E, 'FLOAT'],

            // DOUBLE
            [0.0, 'DOUBLE'],
            [\M_PI, 'DOUBLE'],
            [-\M_E, 'DOUBLE'],

            // SIGNED
            [0, 'SIGNED'],
            [-12345, 'SIGNED'],
            [12345, 'UNSIGNED'],

            // UNSIGNED
            [0, 'UNSIGNED'],
            [12345, 'UNSIGNED'],
        ];
    }

    /**
     * @dataProvider provideDataAndTypes
     */
    public function testDateType(int|float|string|null $expected, string $type): void
    {
        $result = $this->connection->execute("SELECT CAST(:expected AS $type) AS data", ['expected' => $expected]);

        foreach ($result as $row) {
            $data = $row['data'];

            if (\is_float($expected)) {
                self::assertEqualsWithDelta($expected, $data, 1E-5);
            } else {
                self::assertSame($expected, $data);
            }
        }
    }

    public function provideJsonData(): array
    {
        return [
            'object' => [<<<JSON
            {
                "object": {"key": "value"},
                "array": [1, 2, 3],
                "string": "value",
                "integer": 42,
                "float": 3.1415926,
                "boolean": true,
                "null": null
            }
            JSON, false],
            'array' => ['[1, 2, 3]', true],
            'float' => ['3.1415926', true],
            'integer' => ['42', true],
            'boolean' => ['true', true],
            'null' => ['null', true],
        ];
    }

    /**
     * @dataProvider provideJsonData
     */
    public function testJson(mixed $json, bool $same): void
    {
        $result = $this->connection->execute("SELECT CAST(? AS JSON) AS data", [$json]);

        foreach ($result as $row) {
            $expected = \json_decode($json);
            $actual = \json_decode($row['data']);
            if ($same) {
                self::assertSame($expected, $actual);
            } else {
                self::assertEquals($expected, $actual);
            }
        }
    }
}
