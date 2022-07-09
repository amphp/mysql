<?php

namespace Amp\Mysql;

interface MysqlConnection extends MysqlLink
{
    /**
     * Change the active database on the connection.
     */
    public function useDb(string $db): void;

    /**
     * Change the character set used by the connection.
     */
    public function setCharset(string $charset, string $collate): void;
}
