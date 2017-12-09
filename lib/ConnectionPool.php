<?php

namespace Amp\Mysql;

use Amp\Promise;

class ConnectionPool extends AbstractPool {
    const DEFAULT_MAX_CONNECTIONS = 100;

    /** @var \Amp\Mysql\Internal\ConnectionConfig */
    private $config;

    /** @var int */
    private $maxConnections;

    /**
     * @internal Use \Amp\Mysql\pool() instead.
     *
     * @param \Amp\Mysql\Internal\ConnectionConfig $config
     * @param int $maxConnections
     *
     * @throws \Error If $maxConnections is less than 1.
     */
    public function __construct(Internal\ConnectionConfig $config, int $maxConnections = self::DEFAULT_MAX_CONNECTIONS) {
        parent::__construct();

        $this->config = $config;
        $this->maxConnections = $maxConnections;
        if ($this->maxConnections < 1) {
            throw new \Error("Pool must contain at least one connection");
        }
    }

    /**
     * {@inheritdoc}
     */
    protected function createConnection(): Promise {
        return Connection::connect($this->config);
    }

    /**
     * {@inheritdoc}
     */
    public function getMaxConnections(): int {
        return $this->maxConnections;
    }
}
