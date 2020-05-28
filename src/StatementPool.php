<?php

namespace Amp\Mysql;

use Amp\Promise;
use Amp\Sql\Common\StatementPool as SqlStatementPool;
use Amp\Sql\Pool;
use Amp\Sql\Result as SqlResult;
use Amp\Sql\Statement as SqlStatement;
use Amp\Success;
use function Amp\call;

final class StatementPool extends SqlStatementPool implements Statement
{
    private $params = [];

    public function __construct(Pool $pool, Statement $statement, callable $prepare)
    {
        parent::__construct($pool, $statement, $prepare);
    }

    protected function prepare(SqlStatement $statement): Promise
    {
        \assert($statement instanceof Statement);

        return call(function () use ($statement) {
            yield $statement->reset();

            foreach ($this->params as $paramId => $data) {
                $statement->bind($paramId, $data);
            }

            return $statement;
        });
    }

    protected function createResult(SqlResult $result, callable $release): SqlResult
    {
        if (!$result instanceof Result) {
            throw new \TypeError('Result object must be an instance of ' . Result::class);
        }

        return new PooledResult($result, $release);
    }

    public function bind($paramId, $data): void
    {
        if (!\is_int($paramId) && !\is_string($paramId)) {
            throw new \TypeError("Invalid parameter ID type");
        }

        $this->params[$paramId] = $data;
    }

    public function reset(): Promise
    {
        $this->params = [];
        return new Success;
    }

    /**
     * @return Promise<array>
     */
    public function getFields(): Promise
    {
        return call(function () {
            $statement = yield from $this->pop();
            \assert($statement instanceof Statement);
            $fields = yield $statement->getFields();
            $this->push($statement);
            return $fields;
        });
    }
}
