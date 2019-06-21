<?php

namespace Amp\Mysql;

use Amp\Promise;
use Amp\Sql\Common\PooledResultSet as SqlPooledResultSet;

final class PooledResultSet extends SqlPooledResultSet implements ResultSet
{
    /** @var ConnectionResultSet */
    private $result;

    /** @var callable */
    private $release;

    /** @var Promise|null a*/
    private $nextResultPromise;

    /**
     * @param ResultSet $result
     * @param callable  $release
     */
    public function __construct(ResultSet $result, callable $release)
    {
        $this->result = $result;
        $this->release = &$release;

        parent::__construct($this->result, static function () use (&$release) {
            if ($release !== null) {
                $release();
            }
        });
    }

    public function advance(): Promise
    {
        $promise = $this->result->advance();

        $promise->onResolve(function (\Throwable $exception = null, bool $moreResults = null) {
            if ($moreResults || $this->release === null) {
                return;
            }

            $this->nextResultPromise = $this->result->nextResultSet();
            $this->nextResultPromise->onResolve(function (\Throwable $exception = null, bool $moreResults = null) {
                $this->nextResultPromise = null;

                if ($moreResults || $this->release === null) {
                    return;
                }

                $release = $this->release;
                $this->release = null;
                $release();
            });
        });

        return $promise;
    }

    public function nextResultSet(): Promise
    {
        if ($this->nextResultPromise !== null) {
            $nextResultPromise = $this->nextResultPromise;
            $this->nextResultPromise = null;
            return $nextResultPromise;
        }

        $promise = $this->result->nextResultSet();

        $promise->onResolve(function (\Throwable $exception = null, bool $moreResults = null) {
            if ($moreResults || $this->release === null) {
                return;
            }

            $release = $this->release;
            $this->release = null;
            $release();
        });

        return $promise;
    }

    public function getFields(): Promise
    {
        return $this->result->getFields();
    }
}
