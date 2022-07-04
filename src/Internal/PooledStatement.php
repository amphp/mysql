<?php

namespace Amp\Mysql\Internal;

use Amp\Mysql\Result;
use Amp\Mysql\Statement;
use Amp\Sql\Common\PooledStatement as SqlPooledStatement;
use Amp\Sql\Result as SqlResult;

/** @internal */
final class PooledStatement extends SqlPooledStatement implements Statement
{
    private readonly Statement $statement;

    /**
     * @param \Closure():void $release
     */
    public function __construct(Statement $statement, \Closure $release)
    {
        parent::__construct($statement, $release);
        $this->statement = $statement;
    }

    protected function createResult(SqlResult $result, \Closure $release): Result
    {
        \assert($result instanceof Result);
        return new PooledResult($result, $release);
    }

    /**
     * Changes return type to this library's Result type.
     */
    public function execute(array $params = []): Result
    {
        return $this->statement->execute($params);
    }

    public function bind(int|string $paramId, mixed $data): void
    {
        $this->statement->bind($paramId, $data);
    }

    public function getColumnDefinitions(): ?array
    {
        return $this->statement->getColumnDefinitions();
    }

    public function reset(): void
    {
        $this->statement->reset();
    }
}
