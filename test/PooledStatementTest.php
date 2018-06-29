<?php

namespace Amp\Mysql\Test;

use Amp\Delayed;
use Amp\Loop;
use Amp\Mysql\ConnectionConfig;
use Amp\Mysql\Internal\PooledStatement;
use Amp\Mysql\Internal\ResultProxy;
use Amp\Mysql\Pool;
use Amp\Mysql\ResultSet;
use Amp\Mysql\Statement;
use Amp\PHPUnit\TestCase;
use Amp\Success;

class PooledStatementTest extends TestCase
{
    public function testActiveStatementsRemainAfterTimeout()
    {
        Loop::run(function () {
            $pool = new Pool(ConnectionConfig::parseConnectionString('host=host user=user pass=pass'));

            $statement = $this->createMock(Statement::class);
            $statement->method('getQuery')
                ->willReturn('SELECT 1');
            $statement->method('lastUsedAt')
                ->willReturn(\time());
            $statement->expects($this->once())
                ->method('execute');
            $statement->method('reset')
                ->willReturn(new Success);

            $pooledStatement = new PooledStatement($pool, $statement, $this->createCallback(0));

            $this->assertTrue($pooledStatement->isAlive());

            yield new Delayed(1500); // Give timeout watcher enough time to execute.

            $pooledStatement->execute();

            $this->assertTrue($pooledStatement->isAlive());
        });
    }

    public function testIdleStatementsRemovedAfterTimeout()
    {
        Loop::run(function () {
            $pool = new Pool(ConnectionConfig::parseConnectionString('host=host user=user pass=pass'));

            $statement = $this->createMock(Statement::class);
            $statement->method('getQuery')
                ->willReturn('SELECT 1');
            $statement->method('lastUsedAt')
                ->willReturn(0);
            $statement->expects($this->never())
                ->method('execute');
            $statement->method('reset')
                ->willReturn(new Success);

            $prepare = function () {
                $statement = $this->createMock(Statement::class);
                $statement->expects($this->once())
                    ->method('execute')
                    ->willReturn(new Success(new ResultSet(new ResultProxy)));
                $statement->method('reset')
                    ->willReturn(new Success);
                return new Success($statement);
            };

            $pooledStatement = new PooledStatement($pool, $statement, $prepare);

            $this->assertTrue($pooledStatement->isAlive());

            yield new Delayed(1500); // Give timeout watcher enough time to execute and remove mock statement object.

            $result = yield $pooledStatement->execute();

            $this->assertInstanceOf(ResultSet::class, $result);

            $this->assertTrue($pooledStatement->isAlive());
        });
    }
}
