<?php

namespace Amp\Mysql;

use Amp\Promise;
use Amp\Sql\Common\PooledStatement as SqlPooledStatement;
use Amp\Sql\Result as SqlResult;

final class PooledStatement extends SqlPooledStatement implements Statement
{
    private $statement;

    public function __construct(Statement $statement, callable $release)
    {
        parent::__construct($statement, $release);
        $this->statement = $statement;
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
        $this->statement->bind($paramId, $data);
    }

    public function getFields(): Promise
    {
        return $this->statement->getFields();
    }

    public function reset(): Promise
    {
        return $this->statement->reset();
    }
}
