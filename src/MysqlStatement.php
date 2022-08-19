<?php

namespace Amp\Mysql;

use Amp\Sql\Statement;

interface MysqlStatement extends Statement
{
    public function execute(array $params = []): MysqlResult;

    /**
     * @param int|string $paramId Parameter ID or name.
     * @param mixed $data Data to bind to parameter.
     *
     * @throws \Error If $paramId is not an int or string, or the position does not exist.
     */
    public function bind(int|string $paramId, mixed $data): void;

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
