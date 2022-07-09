<?php

namespace Amp\Mysql;

use Amp\Sql\Executor;

/**
 * @extends Executor<MysqlResult, MysqlStatement>
 */
interface MysqlExecutor extends Executor
{
    /**
     * @return MysqlResult Result object specific to this library.
     */
    public function query(string $sql): MysqlResult;

    /**
     * @return MysqlStatement Statement object specific to this library.
     */
    public function prepare(string $sql): MysqlStatement;

    /**
     * @return MysqlResult Result object specific to this library.
     */
    public function execute(string $sql, array $params = []): MysqlResult;
}
