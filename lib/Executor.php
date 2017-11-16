<?php

namespace Amp\Mysql;

use Amp\Promise;

interface Executor {
    /**
     * @param string $sql SQL query to execute.
     *
     * @return \Amp\Promise<\Amp\Mysql\CommandResult|\Amp\Mysql\ResultSet>
     *
     * @throws \Amp\Mysql\FailureException If the operation fails due to unexpected condition.
     * @throws \Amp\Mysql\ConnectionException If the connection to the database is lost.
     * @throws \Amp\Mysql\QueryError If the operation fails due to an error in the query (such as a syntax error).
     */
    public function query(string $sql): Promise;

    /**
     * @param string $sql SQL query to prepare.
     *
     * @return \Amp\Promise<\Amp\Mysql\Statement>
     *
     * @throws \Amp\Mysql\FailureException If the operation fails due to unexpected condition.
     * @throws \Amp\Mysql\ConnectionException If the connection to the database is lost.
     * @throws \Amp\Mysql\QueryError If the operation fails due to an error in the query (such as a syntax error).
     */
    public function prepare(string $sql): Promise;

    /**
     * @param string $sql SQL query to prepare and execute.
     * @param mixed[] $params Query parameters.
     *
     * @return \Amp\Promise<\Amp\Mysql\CommandResult|\Amp\Mysql\ResultSet>
     *
     * @throws \Amp\Mysql\FailureException If the operation fails due to unexpected condition.
     * @throws \Amp\Mysql\ConnectionException If the connection to the database is lost.
     * @throws \Amp\Mysql\QueryError If the operation fails due to an error in the query (such as a syntax error).
     */
    public function execute(string $sql, array $params = []): Promise;

    /**
     * Closes the executor. No further queries may be performed.
     */
    public function close();
}
