<?php

namespace Amp\Mysql\Test;

use Amp\Mysql\MysqlConnection;
use Amp\Mysql\SocketMysqlConnector;

class MysqlDataTypeTest extends MysqlTestCase
{
    private readonly MysqlConnection $connection;

    private readonly \DateTimeImmutable $now;

    public function setUp(): void
    {
        parent::setUp();

        $this->connection = (new SocketMysqlConnector)->connect($this->getConfig());
        $this->now = new \DateTimeImmutable();
    }

    protected function assertResult(int|float|string|null $expected, string $type): void
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

    public function testDate(): void
    {
        $this->assertResult('1970-01-01', 'DATE');
        $this->assertResult('2020-03-17', 'DATE');
        $this->assertResult($this->now->format('Y-m-d'), 'DATE');
    }

    public function testDateTime(): void
    {
        $this->assertResult('1970-01-01 00:00:00', 'DATETIME(6)');
        $this->assertResult($this->now->format('Y-m-d H:i:s.u'), 'DATETIME(6)');
    }

    public function testTime(): void
    {
        $this->assertResult('184:15:56.271282', 'TIME(6)');
        $this->assertResult($this->now->format('H:i:s.u'), 'TIME(6)');
    }

    public function testFloatAndDouble(): void
    {
        $double = 3.1415926;
        $this->assertResult($double, 'FLOAT');
        $this->assertResult($double, 'DOUBLE');
    }

    public function testInteger(): void
    {
        $this->assertResult(-12345, 'SIGNED');
        $this->assertResult(12345, 'UNSIGNED');
    }

    public function provideJsonData(): \Generator
    {
        yield 'object' => [<<<JSON
            {
                "object": {"key": "value"},
                "array": [1, 2, 3],
                "string": "value",
                "integer": 42,
                "float": 3.1415926,
                "boolean": true,
                "null": null
            }
            JSON, false];

        yield 'array' => ['[1, 2, 3]', true];

        yield 'float' => ['3.1415926', true];

        yield 'integer' => ['42', true];

        yield 'boolean' => ['true', true];

        yield 'null' => ['null', true];
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
