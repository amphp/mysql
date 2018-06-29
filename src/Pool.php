<?php

namespace Amp\Mysql;

use Amp\Sql\AbstractPool;
use Amp\Sql\Connector;

final class Pool extends AbstractPool
{
    protected function createDefaultConnector(): Connector
    {
        return connector();
    }
}
