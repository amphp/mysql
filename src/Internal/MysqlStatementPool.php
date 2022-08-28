<?php

namespace Amp\Mysql\Internal;

use Amp\Mysql\MysqlResult;
use Amp\Mysql\MysqlStatement;
use Amp\Sql\Common\StatementPool as SqlStatementPool;
use Amp\Sql\Result as SqlResult;

/**
 * @internal
 * @extends SqlStatementPool<MysqlResult, MysqlStatement>
 */
final class MysqlStatementPool extends SqlStatementPool implements MysqlStatement
{
    private array $params = [];

    protected function pop(): MysqlStatement
    {
        $statement = parent::pop();

        try {
            \assert($statement instanceof MysqlStatement);

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

    protected function createResult(SqlResult $result, \Closure $release): MysqlResult
    {
        if (!$result instanceof MysqlResult) {
            throw new \TypeError('Result object must be an instance of ' . MysqlResult::class);
        }

        return new MysqlPooledResult($result, $release);
    }

    public function execute(array $params = []): MysqlResult
    {
        return parent::execute($params);
    }

    public function bind(int|string $paramId, string $data): void
    {
        $prior = $this->params[$paramId] ?? '';
        $this->params[$paramId] = $prior . $data;
    }

    public function reset(): void
    {
        $this->params = [];
    }

    public function getColumnDefinitions(): array
    {
        $statement = parent::pop();
        $columns = $statement->getColumnDefinitions();
        $this->push($statement);
        return $columns;
    }

    public function getParameterDefinitions(): array
    {
        $statement = parent::pop();
        $parameters = $statement->getParameterDefinitions();
        $this->push($statement);
        return $parameters;
    }
}
