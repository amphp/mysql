<?php declare(strict_types=1);

namespace Amp\Mysql\Test;

use Amp\Mysql\MysqlConfig;
use Amp\Mysql\SocketMysqlConnector;
use Amp\PHPUnit\AsyncTestCase;

abstract class MysqlTestCase extends AsyncTestCase
{
    private static ?bool $isMariaDb = null;

    protected function getConfig(bool $useCompression = false): MysqlConfig
    {
        $config = MysqlConfig::fromAuthority(DB_HOST, DB_USER, DB_PASS, 'test');
        if ($useCompression) {
            $config = $config->withCompression();
        }

        return $config;
    }

    protected function isMariaDb(): bool
    {
        if (self::$isMariaDb !== null) {
            return self::$isMariaDb;
        }

        $db = (new SocketMysqlConnector)->connect($this->getConfig());
        $version = '';
        foreach ($db->query('SELECT VERSION() AS v') as $row) {
            $version = (string) $row['v'];
        }
        $db->close();

        return self::$isMariaDb = \str_contains($version, 'MariaDB');
    }
}
