<?php declare(strict_types=1);

namespace Amp\Mysql;

interface MysqlConnection extends MysqlLink
{
    /**
     * Change the active database on the connection.
     */
    public function useDatabase(string $database): void;

    /**
     * Change the character set used by the connection.
     */
    public function useCharacterSet(string $charset, string $collate): void;
}
