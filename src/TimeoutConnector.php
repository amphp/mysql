<?php

namespace Amp\Mysql;

use Amp\Promise;
use Amp\Socket\ClientConnectContext;
use Amp\Sql\ConnectionConfig as SqlConnectionConfig;
use Amp\Sql\Connector;

final class TimeoutConnector implements Connector
{
    const DEFAULT_TIMEOUT = 5000;

    /** @var ClientConnectContext */
    private $context;

    /**
     * @param int $timeout Milliseconds until connections attempts are cancelled.
     */
    public function __construct(int $timeout = self::DEFAULT_TIMEOUT)
    {
        $this->context = (new ClientConnectContext())->withConnectTimeout($timeout);
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Amp\Sql\FailureException If connecting fails.
     */
    public function connect(SqlConnectionConfig $config): Promise
    {
        if (!$config instanceof ConnectionConfig) {
            throw new \TypeError(\sprintf("Must provide an instance of %s to MySQL connectors", ConnectionConfig::class));
        }

        return Connection::connect($config, null, $this->context);
    }
}
