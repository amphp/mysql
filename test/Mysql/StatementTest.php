<?php

use Amp\Mysql\Statement;
use PHPUnit\Framework\TestCase;
use Amp\Mysql\Internal\ResultProxy;

class StatementTest extends TestCase {
    /** @var \Prophecy\Prophecy\ObjectProphecy */
    protected $processor;

    /** @var \Amp\Mysql\Internal\ResultProxy */
    protected $resultProxy;

    public function setUp() {
        $this->processor = $this->prophesize('Amp\Mysql\Internal\Processor');
        $this->resultProxy = new ResultProxy;
    }

    /**
     * @dataProvider provideTestBindDataTypes
     */
    public function testBindDataTypes($data, $expectedException) {
        // arrange
        $query = 'SELECT * FROM test WHERE id = ?';
        $stmtId = 1;
        $paramId = 0;
        $named = [];

        $this->processor->isAlive()->willReturn(true);
        $this->processor->delRef()->shouldBeCalled();
        $this->processor->closeStmt(\Prophecy\Argument::any())->shouldBeCalled();
        $this->resultProxy->columnsToFetch = 1;
        $stmt = new Statement($this->processor->reveal(), $query, $stmtId, $named, $this->resultProxy);

        // assert
        if ($expectedException) {
            $this->expectException($expectedException);
            $this->processor->bindParam($stmtId, \Prophecy\Argument::any(), $data)->shouldNotBeCalled();
        } else {
            $this->addToAssertionCount(1);
            $this->processor->bindParam($stmtId, \Prophecy\Argument::any(), $data)->shouldBeCalled();
        }

        // act
        $stmt->bind($paramId, $data);
    }

    public function provideTestBindDataTypes() {
        return [
            'test scalar' => [
                'data' => 1,
                'expectedException' => false,
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
                'data' => new class { public function __toString() {return '';} },
                'expectedException' => false,
            ],
        ];
    }
}
