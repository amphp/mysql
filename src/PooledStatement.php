<?php

namespace Amp\Mysql;

use Amp\Promise;
use Amp\Sql\Common\PooledStatement as SqlPooledStatement;
use Amp\Sql\ResultSet as SqlResultSet;

final class PooledStatement extends SqlPooledStatement implements Statement
{
    private $statement;

    public function __construct(Statement $statement, callable $release)
    {
        parent::__construct($statement, $release);
        $this->statement = $statement;
    }

    protected function createResultSet(SqlResultSet $resultSet, callable $release): SqlResultSet
    {
        \assert($resultSet instanceof ResultSet);
        return new PooledResultSet($resultSet, $release);
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
