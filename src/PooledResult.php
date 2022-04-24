<?php

namespace Amp\Mysql;

use Amp\Sql\Common\PooledResult as SqlPooledResult;
use Amp\Sql\Result as SqlResult;

final class PooledResult extends SqlPooledResult implements Result
{
    private readonly Result $result;

    /**
     * @param \Closure():void $release
     */
    public function __construct(Result $result, \Closure $release)
    {
        parent::__construct($result, $release);
        $this->result = $result;
    }

    protected function newInstanceFrom(SqlResult $result, \Closure $release): self
    {
        \assert($result instanceof Result);
        return new self($result, $release);
    }

    public function getLastInsertId(): ?int
    {
        return $this->result->getLastInsertId();
    }

    public function getColumnDefinitions(): ?array
    {
        return $this->result->getColumnDefinitions();
    }
}
