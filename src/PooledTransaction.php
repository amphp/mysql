<?php

namespace Amp\Mysql;

use Amp\Promise;
use Amp\Sql\PooledTransaction as SqlPooledTransaction;
use Amp\Sql\ResultSet as SqlResultSet;
use Amp\Sql\Statement as SqlStatement;

final class PooledTransaction extends SqlPooledTransaction
{
    /** @var Transaction|null */
    private $transaction;

    protected function createStatement(SqlStatement $statement, callable $release): SqlStatement
    {
        return new PooledStatement($statement, $release);
    }

    protected function createResultSet(SqlResultSet $resultSet, callable $release): SqlResultSet
    {
        \assert($resultSet instanceof ResultSet);
        return new PooledResultSet($resultSet, $release);
    }

    /**
     * @param Transaction $transaction
     * @param callable    $release
     */
    public function __construct(Transaction $transaction, callable $release)
    {
        parent::__construct($transaction, $release);
        $this->transaction = $transaction;
    }
}
