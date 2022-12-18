<?php

namespace Amp\Mysql\Internal;

use Amp\Mysql\MysqlResult;
use Amp\Mysql\MysqlStatement;
use Amp\Mysql\MysqlTransaction;
use Amp\Sql\Common\StatementPool as SqlStatementPool;
use Amp\Sql\Result as SqlResult;
use Amp\Sql\Statement;

/**
 * @internal
 * @extends SqlStatementPool<MysqlResult, MysqlStatement, MysqlTransaction>
 */
final class MysqlStatementPool extends SqlStatementPool implements MysqlStatement
{
    private array $params = [];

    protected function pop(): MysqlStatement
    {
        $statement = parent::pop();

        try {
            \assert($statement instanceof MysqlStatement);

            foreach ($this->params as $paramId => $data) {
                $statement->bind($paramId, $data);
            }
        } catch (\Throwable $exception) {
            $this->push($statement);
            throw $exception;
        }

        return $statement;
    }

    protected function push(Statement $statement): void
    {
        if ($statement->isClosed()) {
            return;
        }

        $statement->reset();
        parent::push($statement);
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
        parent::push($statement);
        return $columns;
    }

    public function getParameterDefinitions(): array
    {
        $statement = parent::pop();
        $parameters = $statement->getParameterDefinitions();
        parent::push($statement);
        return $parameters;
    }
}
