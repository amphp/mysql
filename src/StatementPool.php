<?php

namespace Amp\Mysql;

use Amp\Sql\Common\StatementPool as SqlStatementPool;
use Amp\Sql\Result as SqlResult;

final class StatementPool extends SqlStatementPool implements Statement
{
    private array $params = [];

    /**
     * @psalm-suppress LessSpecificReturnStatement, MoreSpecificReturnType
     */
    protected function pop(): Statement
    {
        $statement = parent::pop();

        try {
            \assert($statement instanceof Statement);

            $statement->reset();

            foreach ($this->params as $paramId => $data) {
                $statement->bind($paramId, $data);
            }
        } catch (\Throwable $exception) {
            $this->push($statement);
            throw $exception;
        }

        return $statement;
    }

    protected function createResult(SqlResult $result, \Closure $release): Result
    {
        if (!$result instanceof Result) {
            throw new \TypeError('Result object must be an instance of ' . Result::class);
        }

        return new PooledResult($result, $release);
    }

    /**
     * @psalm-suppress LessSpecificReturnStatement, MoreSpecificReturnType
     */
    public function execute(array $params = []): Result
    {
        return parent::execute($params);
    }

    public function bind(int|string $paramId, mixed $data): void
    {
        $this->params[$paramId] = $data;
    }

    public function reset(): void
    {
        $this->params = [];
    }

    public function getColumnDefinitions(): ?array
    {
        $statement = $this->pop();
        $columns = $statement->getColumnDefinitions();
        $this->push($statement);
        return $columns;
    }
}
