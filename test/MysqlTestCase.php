<?php declare(strict_types=1);

namespace Amp\Mysql\Test;

use Amp\Mysql\MysqlConfig;
use Amp\PHPUnit\AsyncTestCase;

abstract class MysqlTestCase extends AsyncTestCase
{
    protected function getConfig(bool $useCompression = false): MysqlConfig
    {
        $config = MysqlConfig::fromAuthority(DB_HOST, DB_USER, DB_PASS, 'test');
        if ($useCompression) {
            $config = $config->withCompression();
        }

        return $config;
    }
}
