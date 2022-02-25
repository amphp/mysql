<?php

namespace Amp\Mysql;

use Amp\Sql\Link as SqlLink;
use Amp\Sql\TransactionIsolation;

interface Link extends Executor, SqlLink
{
    /**
     * @inheritDoc
     *
     * @return Transaction Transaction object specific to this library.
     */
    public function beginTransaction(TransactionIsolation $isolation = TransactionIsolation::COMMITTED): Transaction;
}
