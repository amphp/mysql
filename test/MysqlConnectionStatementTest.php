<?php declare(strict_types=1);

namespace Amp\Mysql\Test;

use Amp\Future;
use Amp\Mysql\Internal\ConnectionProcessor;
use Amp\Mysql\Internal\MysqlCommandResult;
use Amp\Mysql\Internal\MysqlConnectionStatement;
use Amp\Mysql\Internal\MysqlResultProxy;
use Amp\Mysql\MysqlColumnDefinition;
use Amp\Mysql\MysqlDataType;
use Amp\PHPUnit\AsyncTestCase;
use Amp\Sql\SqlConnectionException;
use Revolt\EventLoop;

class MysqlConnectionStatementTest extends AsyncTestCase
{
    /**
     * @dataProvider provideAvailableParameterDefinitions
     */
    public function testExecutionWaitsForParameterDefinitions(int $availableDefinitions): void
    {
        $definitions = [
            new MysqlColumnDefinition('test', 'first', 8, MysqlDataType::LongLong, 0, 0),
            new MysqlColumnDefinition('test', 'second', 8, MysqlDataType::LongLong, 0, 0),
        ];
        $metadata = new MysqlResultProxy(1, 2);
        $metadata->params = \array_slice($definitions, 0, $availableDefinitions);
        $result = new MysqlCommandResult(0, 0);
        $processor = $this->createMock(ConnectionProcessor::class);
        $processor->expects($this->once())
            ->method('execute')
            ->with(1, 'SELECT ?, ?', $definitions, [], [1, 2])
            ->willReturn(Future::complete($result));
        $statement = new MysqlConnectionStatement($processor, 'SELECT ?, ?', 1, [], $metadata);

        EventLoop::queue(static function () use ($metadata, $definitions): void {
            $metadata->params = $definitions;
            $metadata->markDefinitionsFetched();
        });

        try {
            $this->assertSame($result, $statement->execute([1, 2]));
        } finally {
            $statement->close();
        }
    }

    public static function provideAvailableParameterDefinitions(): array
    {
        return [
            'no definitions received' => [0],
            'partial definitions received' => [1],
            'all definitions received' => [2],
        ];
    }

    public function testMetadataFailurePropagatesWithoutExecution(): void
    {
        $metadata = new MysqlResultProxy(1, 2);
        $processor = $this->createMock(ConnectionProcessor::class);
        $processor->expects($this->never())->method('execute');
        $statement = new MysqlConnectionStatement($processor, 'SELECT ?, ?', 1, [], $metadata);
        $exception = new SqlConnectionException('Connection lost during statement preparation');

        EventLoop::queue(static fn () => $metadata->error($exception));

        $this->expectExceptionObject($exception);

        try {
            $statement->execute([1, 2]);
        } finally {
            $statement->close();
        }
    }
}
