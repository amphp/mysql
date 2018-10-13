<?php

namespace Amp\Mysql;

use Amp\Promise;
use Amp\Sql\Common\PooledResultSet as SqlPooledResultSet;

final class PooledResultSet extends SqlPooledResultSet implements ResultSet
{
    /** @var ConnectionResultSet */
    private $result;

    /**
     * @param ResultSet $result
     * @param callable            $release
     */
    public function __construct(ResultSet $result, callable $release)
    {
        parent::__construct($result, $release);
        $this->result = $result;
    }

    public function nextResultSet(): Promise
    {
        return $this->result->nextResultSet();
    }

    public function getFields(): Promise
    {
        return $this->result->getFields();
    }
}
