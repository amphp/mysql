<?php

namespace Amp\Mysql;

use Amp\Sql\Common\PooledStatement as SqlPooledStatement;
use Amp\Sql\Result as SqlResult;

final class PooledStatement extends SqlPooledStatement implements Statement
{
    private readonly Statement $statement;

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

    public function getFields(): ?array
    {
        return $this->statement->getFields();
    }

    public function reset(): void
    {
        $this->statement->reset();
    }
}
