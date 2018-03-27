<?php

namespace Amp\Mysql;

use Amp\Promise;

interface Statement extends Operation {
    /**
     * @param mixed[] $params Data to bind to parameters.
     *
     * @return \Amp\Promise
     *
     * @throws \Error If a named parameter was not bound or a parameter is missing.
     */
    public function execute(array $params = []): Promise;

    /**
     * @param int|string $paramId Parameter ID or name.
     * @param mixed $data Data to bind to parameter.
     */
    public function bind($param, $data);

    /**
     * @return string The SQL string used to prepare the statement.
     */
    public function getQuery(): string;

    /**
     * @return \Amp\Promise<array>
     */
    public function getFields(): Promise;

    public function reset();

    public function close();
}
