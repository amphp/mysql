<?php

namespace Amp\Mysql;

use Amp\Promise;
use Amp\Sql\Common\PooledResult as SqlPooledResult;
use Amp\Sql\Result as SqlResult;

final class PooledResult extends SqlPooledResult implements Result
{
    /** @var Result */
    private $result;

    /**
     * @param Result $result
     * @param callable  $release
     */
    public function __construct(Result $result, callable $release)
    {
        parent::__construct($result, $release);
        $this->result = $result;
    }

    protected function newInstanceFrom(SqlResult $result, callable $release): SqlPooledResult
    {
        if (!$result instanceof Result) {
            throw new \TypeError('Result object must be an instance of ' . Result::class);
        }

        return new self($result, $release);
    }

    public function getLastInsertId(): ?int
    {
        return $this->result->getRowCount();
    }

    public function getFields(): Promise
    {
        return $this->result->getFields();
    }
}
