<?php

namespace Amp\Mysql\Internal;

use Amp\Socket\ClientTlsContext;
use Amp\Struct;

class ConnectionConfig {
    use Struct;

    const KEY_MAP = ['username' => 'user', 'password' => 'pass', 'database' => 'db', 'dbname' => 'db'];

    /* <domain/IP-string>(:<port>) */
    public $host;
    /* Can be resolved with resolveHost() method */
    public $resolvedHost;
    public $user;
    public $pass;
    public $db;

    public $useCompression = false;

    /** @var \Amp\Socket\ClientTlsContext|null Null for no ssl   */
    public $ssl;

    /** @var int Charset id @see 14.1.4 in mysql manual */
    public $binCharset = 45; // utf8mb4_general_ci
    public $charset = "utf8mb4";
    public $collate = "utf8mb4_general_ci";

    /* private key to use for sha256_password auth method */
    public $key;

    private function __construct() {
        // Private to force usage of static constructor.
    }

    /**
     * @param string $connStr
     * @param \Amp\Socket\ClientTlsContext|null $sslOptions
     *
     * @return \Amp\Mysql\Internal\ConnectionConfig
     *
     * @throws \Error If a host, user, and password are not provided.
     */
    public static function parseConnectionString(string $connStr, ClientTlsContext $sslOptions = null): self {
        $config = new self;

        $params = \explode(";", $connStr);

        if (\count($params) === 1) { // Attempt to explode on a space if no ';' are found.
            $params = \explode(" ", $connStr);
        }

        foreach ($params as $param) {
            list($key, $value) = \array_map("trim", \explode("=", $param, 2) + [1 => null]);

            if (isset(self::KEY_MAP[$key])) {
                $key = self::KEY_MAP[$key];
            }

            $config->{$key} = $value;
        }

        if (!isset($config->host, $config->user, $config->pass)) {
            throw new \Error("Required parameters host, user and pass need to be passed in connection string");
        }

        $config->useCompression = $config->useCompression && $config->useCompression !== "false";

        $config->ssl = $sslOptions;

        $config->resolveHost();

        return $config;
    }

    private function resolveHost() {
        $index = \strpos($this->host, ':');

        if ($index === false) {
            $this->resolvedHost = "tcp://{$this->host}:3306";
        } elseif ($index === 0) {
            $this->resolvedHost = "tcp://localhost:" . (int) \substr($this->host, 1);
            $this->host = "localhost";
        } else {
            list($host, $port) = \explode(':', $this->host, 2);
            $this->host = $host;
            $this->resolvedHost = "tcp://$host:" . (int) $port;
        }
    }
}
