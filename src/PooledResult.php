<?php

namespace Amp\Mysql;

use Amp\Sql\Common\PooledResult as SqlPooledResult;
use Amp\Sql\Result as SqlResult;

final class PooledResult extends SqlPooledResult implements Result
{
    private Result $result;

    /**
     * @param Result $result
     * @param callable  $release
     */
    public function __construct(Result $result, callable $release)
    {
        parent::__construct($result, $release);
        $this->result = $result;
    }

    protected function newInstanceFrom(SqlResult $result, callable $release): PooledResult
    {
        \assert($result instanceof Result);
        return new self($result, $release);
    }

    public function getLastInsertId(): ?int
    {
        return $this->result->getRowCount();
    }

    public function getFields(): ?array
    {
        return $this->result->getFields();
    }
}
