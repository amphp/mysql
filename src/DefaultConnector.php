<?php

namespace Amp\Mysql;

use Amp\Promise;
use Amp\Socket;
use function Amp\call;

final class DefaultConnector implements Connector {
    /**
     * {@inheritdoc}
     *
     * @throws \Amp\Mysql\FailureException If connecting fails.
     */
    public function connect(ConnectionConfig $config): Promise {
        return call(function () use ($config) {
            $socket = yield Socket\connect($config->getResolvedHost());
            return Connection::connect($socket, $config);
        });
    }
}
