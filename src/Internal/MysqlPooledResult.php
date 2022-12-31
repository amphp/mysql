<?php declare(strict_types=1);

namespace Amp\Mysql\Internal;

use Amp\Mysql\MysqlResult;
use Amp\Sql\Common\PooledResult;
use Amp\Sql\Result;

/**
 * @internal
 * @psalm-import-type TRow from MysqlResult
 * @extends PooledResult<TRow, MysqlResult>
 */
final class MysqlPooledResult extends PooledResult implements MysqlResult
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

    protected function newInstanceFrom(Result $result, \Closure $release): self
    {
        \assert($result instanceof MysqlResult);
        return new self($result, $release);
    }

    public function getNextResult(): ?MysqlResult
    {
        return parent::getNextResult();
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
