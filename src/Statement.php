<?php

namespace Amp\Mysql;

use Amp\Promise;

interface Statement extends \Amp\Sql\Statement {
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
     *
     * @throws \Error If $paramId is not an int or string, or the position does not exist.
     */
    public function bind($paramId, $data);

    /**
     * @return bool True if the statement can still be executed, false if the connection has died.
     */
    public function isAlive(): bool;

    /**
     * @return string The SQL string used to prepare the statement.
     */
    public function getQuery(): string;

    /**
     * @return \Amp\Promise<array>
     */
    public function getFields(): Promise;

    /**
     * Reset statement to state just after preparing.
     *
     * @return \Amp\Promise<null>
     */
    public function reset(): Promise;

    /**
     * @return int Timestamp of when the statement was last used.
     */
    public function lastUsedAt(): int;
}
