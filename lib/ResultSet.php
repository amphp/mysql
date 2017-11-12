<?php

namespace Amp\Mysql;

use Amp\Deferred;
use Amp\Promise;
use Amp\Success;

class ResultSet {
    private $connInfo;
    private $result;

    public function __construct(ConnectionState $state, Internal\ResultProxy $result) {
        $this->connInfo = $state;
        $this->result = $result;
    }

    public function getFields(): Promise {
        if ($this->result->state >= Internal\ResultProxy::COLUMNS_FETCHED) {
            return new Success($this->result->columns);
        } else {
            $deferred = new Deferred;
            $this->result->deferreds[Internal\ResultProxy::COLUMNS_FETCHED][] = [$deferred, &$this->result->columns, null];
            return $deferred->promise();
        }
    }

    public function rowCount(): Promise {
        if ($this->result->state == Internal\ResultProxy::ROWS_FETCHED) {
            return new Success(count($this->result->rows));
        } else {
            $deferred = new Deferred;
            $this->result->deferreds[Internal\ResultProxy::ROWS_FETCHED][] = [$deferred, null, function () {
                return count($this->result->rows);
            }];
            return $deferred->promise();
        }
    }

    protected function genericFetchAll(callable $cb): Promise {
        if ($this->result->state == Internal\ResultProxy::ROWS_FETCHED) {
            return new Success($cb($this->result->rows));
        } else {
            $deferred = new Deferred;
            $this->result->deferreds[Internal\ResultProxy::ROWS_FETCHED][] = [$deferred, &$this->result->rows, $cb];
            return $deferred->promise();
        }
    }

    public function fetchRows(): Promise {
        return $this->genericFetchAll(function($rows) {
            return $rows ?: [];
        });
    }

    public function fetchAssocs(): Promise {
        return $this->genericFetchAll(function($rows) {
            $names = array_column($this->result->columns, "name");
            return array_map(function($row) use ($names) {
                return array_combine($names, $row);
            }, $rows ?: []);
        });
    }
    
    public function fetchObjects(): Promise {
        return $this->genericFetchAll(function($rows) {
            $names = array_column($this->result->columns, "name");
            return array_map(function($row) use ($names) {
                return (object) array_combine($names, $row);
            }, $rows ?: []);
        });
    }

    public function fetchAll(): Promise {
        return $this->genericFetchAll(function($rows) {
            $names = array_column($this->result->columns, "name");
            return array_map(function($row) use ($names) {
                return array_combine($names, $row) + $row;
            }, $rows ?: []);
        });
    }

    protected function genericFetch(callable $cb = null): Promise {
        if ($this->result->userFetched < $this->result->fetchedRows) {
            $row = $this->result->rows[$this->result->userFetched++];
            return new Success($cb ? $cb($row) : $row);
        } elseif ($this->result->state == Internal\ResultProxy::ROWS_FETCHED) {
            return new Success(null);
        } else {
            $deferred = new Deferred;

            /* We need to increment the internal counter, else the next time genericFetch is called,
             * it'll simply return the row we fetch here instead of fetching a new row
             * since callback order on promises isn't defined, we can't do this via onResolve() */
            $incRow = function ($row) use ($cb) {
                $this->result->userFetched++;
                return $cb && $row ? $cb($row) : $row;
            };

            $this->result->deferreds[Internal\ResultProxy::SINGLE_ROW_FETCH][] = [$deferred, null, $incRow];
            return $deferred->promise();
        }
    }

    public function fetchRow(): Promise {
        return $this->genericFetch();
    }

    public function fetchAssoc(): Promise {
        return $this->genericFetch(function ($row) {
            return array_combine(array_column($this->result->columns, "name"), $row);
        });
    }
    
    public function fetchObject(): Promise {
        return $this->genericFetch(function ($row) {
            return (object) array_combine(array_column($this->result->columns, "name"), $row);
        });
    }

    public function fetch(): Promise {
        return $this->genericFetch(function ($row) {
            return array_combine(array_column($this->result->columns, "name"), $row) + $row;
        });
    }

    public function getConnInfo(): ConnectionState {
        return clone $this->connInfo;
    }

    public function next(): Promise {
        $deferred = $this->result->next ?: $this->result->next = new Deferred;
        return $deferred->promise();
    }
}
