<?php

namespace Amp\Mysql\Internal;

use Amp\Mysql\MysqlResult;
use Amp\Sql\Common\PooledResult as SqlPooledResult;
use Amp\Sql\Result as SqlResult;

/** @internal */
final class MysqlPooledResult extends SqlPooledResult implements MysqlResult
{
    private readonly MysqlResult $result;

    /**
     * @param \Closure():void $release
     */
    public function __construct(MysqlResult $result, \Closure $release)
    {
        parent::__construct($result, $release);
        $this->result = $result;
    }

    protected function newInstanceFrom(SqlResult $result, \Closure $release): self
    {
        \assert($result instanceof MysqlResult);
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
