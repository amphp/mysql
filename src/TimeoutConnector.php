<?php

namespace Amp\Mysql;

use Amp\Promise;
use Amp\Socket;
use Amp\TimeoutCancellationToken;
use function Amp\call;

final class TimeoutConnector implements Connector {
    const DEFAULT_TIMEOUT = 5000;

    /** @var int */
    private $timeout;

    /**
     * @param int $timeout Milliseconds until connections attempts are cancelled.
     */
    public function __construct(int $timeout = self::DEFAULT_TIMEOUT) {
        $this->timeout = $timeout;
    }


    /**
     * {@inheritdoc}
     *
     * @throws \Amp\Mysql\FailureException If connecting fails.
     */
    public function connect(ConnectionConfig $config): Promise {
        return call(function () use ($config) {
            $token = new TimeoutCancellationToken($this->timeout);
            $socket = yield Socket\connect($config->getResolvedHost(), null, $token);
            return Connection::connect($socket, $config);
        });
    }
}
