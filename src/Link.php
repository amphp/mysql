<?php

namespace Amp\Mysql;

use Amp\Sql\Link as SqlLink;

interface Link extends Executor, SqlLink
{
    /**
     * @inheritDoc
     *
     * @return Transaction Transaction object specific to this library.
     */
    public function beginTransaction(int $isolation = Transaction::ISOLATION_COMMITTED): Transaction;
}
