<?php

namespace Amp\Mysql\Internal;

use Amp\Mysql\MysqlResult;
use Amp\Mysql\MysqlStatement;
use Amp\Sql\Common\PooledStatement;
use Amp\Sql\Result as SqlResult;

/**
 * @internal
 * @extends PooledStatement<MysqlResult, MysqlStatement>
 */
final class MysqlPooledStatement extends PooledStatement implements MysqlStatement
{
    private readonly MysqlStatement $statement;

    /**
     * @param \Closure():void $release
     */
    public function __construct(MysqlStatement $statement, \Closure $release)
    {
        parent::__construct($statement, $release);
        $this->statement = $statement;
    }

    protected function createResult(SqlResult $result, \Closure $release): MysqlResult
    {
        \assert($result instanceof MysqlResult);
        return new MysqlPooledResult($result, $release);
    }

    /**
     * Changes return type to this library's Result type.
     */
    public function execute(array $params = []): MysqlResult
    {
        return parent::execute($params);
    }

    public function bind(int|string $paramId, string $data): void
    {
        $this->statement->bind($paramId, $data);
    }

    public function getColumnDefinitions(): array
    {
        return $this->statement->getColumnDefinitions();
    }

    public function getParameterDefinitions(): array
    {
        return $this->statement->getParameterDefinitions();
    }

    public function reset(): void
    {
        $this->statement->reset();
    }
}
