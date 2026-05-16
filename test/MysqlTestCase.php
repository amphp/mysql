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
        return self::$isMariaDb ??= \str_contains($this->getDbVersion(), 'MariaDB');
    }

    protected function getDbVersion(): string
    {
        $db = (new SocketMysqlConnector())->connect($this->getConfig());
        $version = $db->query('SELECT VERSION() AS v')->fetchRow()['v'] ?? '';
        $db->close();

        return $version;
    }
}
