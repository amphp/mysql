<?php

namespace Amp\Mysql\Test;

use Amp\Mysql\Internal\ConnectionStatement;
use Amp\Mysql\Internal\Processor;
use Amp\Mysql\Internal\ResultProxy;
use Amp\PHPUnit\AsyncTestCase;
use PHPUnit\Framework\MockObject\MockObject;

class StatementTest extends AsyncTestCase
{
    /** @var MockObject&Processor */
    protected Processor $processor;

    protected ResultProxy $resultProxy;

    public function setUp(): void
    {
        parent::setUp();

        $this->processor = $this->createMock(Processor::class);
        $this->resultProxy = new ResultProxy;
    }

    /**
     * @dataProvider provideTestBindDataTypes
     */
    public function testBindDataTypes($data, $expectedException)
    {
        // arrange
        $query = 'SELECT * FROM test WHERE id = ?';
        $stmtId = 1;
        $paramId = 0;
        $named = [];

        $this->processor->method('isClosed')
            ->willReturn(false);
        $this->processor->expects($this->once())
            ->method('unreference');
        $this->processor->expects($this->once())
            ->method('closeStmt');

        $this->resultProxy->columnsToFetch = 1;
        $stmt = new ConnectionStatement($this->processor, $query, $stmtId, $named, $this->resultProxy);

        // assert
        if ($expectedException) {
            $this->expectException($expectedException);
            $this->processor->expects($this->never())
                ->method('bindParam')
                ->with($stmtId, $this->anything(), $data);
        } else {
            $this->processor->expects($this->once())
                ->method('bindParam')
                ->with($stmtId, $this->anything(), $data);
        }

        $this->assertSame($query, $stmt->getQuery());

        // act
        $stmt->bind($paramId, $data);
    }

    public function provideTestBindDataTypes()
    {
        return [
            'test scalar' => [
                'data' => 1,
                'expectedException' => null,
            ],
            'test object' => [
                'data' => (object) [],
                'expectedException' => 'TypeError',
            ],
            'test array' => [
                'data' => [],
                'expectedException' => 'TypeError',
            ],
            'test object with __toString defined' => [
                'data' => new class {
                    public function __toString()
                    {
                        return '';
                    }
                },
                'expectedException' => null,
            ],
        ];
    }
}
