<?php

namespace Amp\Mysql;

use Amp\Promise;

interface Connector {
    /**
     * @param ConnectionConfig $config
     *
     * @return Promise<Connection>
     */
    public function connect(ConnectionConfig $config): Promise;
}
