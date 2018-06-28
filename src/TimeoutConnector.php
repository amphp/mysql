<?php

namespace Amp\Mysql;

use Amp\Promise;
use Amp\Socket;
use Amp\Mysql\ConnectionConfig as MysqlConnectionConfig;
use Amp\Sql\ConnectionConfig;
use Amp\Sql\ConnectionException;
use Amp\Sql\Connector;
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
     * @throws \Amp\Sql\FailureException If connecting fails.
     */
    public function connect(ConnectionConfig $config): Promise {
        if (! $config instanceof MysqlConnectionConfig) {
            throw new ConnectionException('Expected an instance of ' . MysqlConnectionConfig::class);
        }

        return call(function () use ($config) {
            static $connectContext;

            $connectContext = $connectContext ?? (new Socket\ClientConnectContext())->withTcpNoDelay();
            $token = new TimeoutCancellationToken($this->timeout);

            $socket = yield Socket\connect($config->connectionString(), $connectContext, $token);

            $connection = new Connection($socket, $config);

            yield $connection->connect();

            return $connection;
        });
    }
}
