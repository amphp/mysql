<?php

namespace Amp\Mysql;

use Amp\Sql\Executor as SqlExecutor;

interface Executor extends SqlExecutor
{
    /**
     * @inheritDoc
     *
     * @return Result Result object specific to this library.
     */
    public function query(string $sql): Result;

    /**
     * @inheritDoc
     *
     * @return Result Result object specific to this library.
     */
    public function prepare(string $sql): Statement;

    /**
     * @inheritDoc
     *
     * @return Result Result object specific to this library.
     */
    public function execute(string $sql, array $params = []): Result;
}
