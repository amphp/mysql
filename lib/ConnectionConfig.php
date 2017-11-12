<?php

namespace Amp\Mysql;

use Amp\Struct;

class ConnectionConfig {
    use Struct;

    /* <domain/IP-string>(:<port>) */
    public $host;
    /* Can be resolved with resolveHost() method */
    public $resolvedHost;
    public $user;
    public $pass;
    public $db = null;

    public $useCompression = false;

    /** @var \Amp\Socket\ClientTlsContext|null Null for no ssl   */
    public $ssl = null;

    /** @var callable|null Called when finished fetching all pending data */
    public $ready = null;
    /** @var callable|null Called when Connection re-enters fetching mode */
    public $busy = null;
    /** @var callable|null Called when connection broke, first param is Connection object, second param whether it happened before successful auth */
    public $restore = null;
    /* @var bool Throw exceptions for general mysql errors [not connection or protocol specific] or just return false in the Promises ? */
    public $exceptions = true;
    /** @var int Charset id @see 14.1.4 in mysql manual */
    public $binCharset = 45; // utf8mb4_general_ci
    public $charset = "utf8mb4";
    public $collate = "utf8mb4_general_ci";
    /* private key to use for sha256_password auth method */
    public $key = null;

    public function resolveHost() {
        $index = strpos($this->host, ':');

        if ($index === false) {
            $this->resolvedHost = "tcp://{$this->host}:3306";
        } else if ($index === 0) {
            $this->host = "localhost";
            $this->resolvedHost = "tcp://localhost:" . (int) substr($this->host, 1);
        } else {
            list($host, $port) = explode(':', $this->host, 2);
            $this->host = $host;
            $this->resolvedHost = "tcp://$host:" . (int) $port;
        }
    }
}
