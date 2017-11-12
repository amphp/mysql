<?php

namespace Amp\Mysql;

use Amp\Promise;

interface Executor {
    /**
     * @param string $sql
     *
     * @return \Amp\Promise<\Amp\Postgres\CommandResult|\Amp\Postgres\TupleResult>
     *
     * @throws \Amp\Mysql\FailureException If the operation fails due to unexpected condition.
     * @throws \Amp\Mysql\ConnectionException If the connection to the database is lost.
     * @throws \Amp\Mysql\QueryError If the operation fails due to an error in the query (such as a syntax error).
     */
    public function query(string $sql): Promise;

    /**
     * @param string $sql
     *
     * @return \Amp\Promise<\Amp\Postgres\Statement>
     *
     * @throws \Amp\Mysql\FailureException If the operation fails due to unexpected condition.
     * @throws \Amp\Mysql\ConnectionException If the connection to the database is lost.
     * @throws \Amp\Mysql\QueryError If the operation fails due to an error in the query (such as a syntax error).
     */
    public function prepare(string $sql): Promise;

    /**
     * Closes the executor. No further queries may be performed.
     */
    public function close();
}
