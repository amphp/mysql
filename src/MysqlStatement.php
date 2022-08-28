<?php

namespace Amp\Mysql;

use Amp\Sql\Statement;

interface MysqlStatement extends Statement
{
    public function execute(array $params = []): MysqlResult;

    /**
     * @param int|string $paramId Parameter ID or name.
     * @param string $data Data to bind to parameter.
     *
     * @throws \Error If $paramId does not exist.
     */
    public function bind(int|string $paramId, string $data): void;

    /**
     * @return list<MysqlColumnDefinition>
     */
    public function getColumnDefinitions(): array;

    /**
     * @return list<MysqlColumnDefinition>
     */
    public function getParameterDefinitions(): array;

    /**
     * Reset statement to state just after preparing.
     */
    public function reset(): void;
}
