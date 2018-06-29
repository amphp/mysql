<?php

namespace Amp\Mysql;

use Amp\Socket\ClientTlsContext;
use Amp\Sql\ConnectionConfig as SqlConnectionConfig;

final class ConnectionConfig implements SqlConnectionConfig
{
    const BIN_CHARSET = 45; // utf8mb4_general_ci

    const ALLOWED_KEYS = [
        'host',
        'user',
        'pass',
        'db',
        'useCompression',
        'key',
    ];

    const KEY_MAP = [
        'username' => 'user',
        'password' => 'pass',
        'database' => 'db',
        'dbname' => 'db',
        'compress' => 'useCompression',
    ];

    const DEFAULT_CHARSET = "utf8mb4";
    const DEFAULT_COLLATE = "utf8mb4_general_ci";

    /* string <domain/IP-string>(:<port>) */
    private $host;

    /** @var string */
    private $user;

    /** @var string */
    private $pass;

    /** @var string|null */
    private $db;

    /** @var bool */
    private $useCompression = false;

    /** @var \Amp\Socket\ClientTlsContext|null Null for no ssl   */
    private $ssl;

    /** @var string */
    private $charset = "utf8mb4";
    /** @var string  */
    private $collate = "utf8mb4_general_ci";

    /* private key to use for sha256_password auth method */
    private $key;

    /** @var string|null */
    private $resolvedHost;

    private function __construct()
    {
        // Private to force usage of static constructor.
    }

    /**
     * @param string $connectionString
     * @param \Amp\Socket\ClientTlsContext|null $sslOptions
     *
     * @return self
     *
     * @throws \Error If a host, user, and password are not provided.
     */
    public static function parseConnectionString(string $connectionString, ClientTlsContext $sslOptions = null): self
    {
        $config = new self;

        $params = \explode(";", $connectionString);

        if (\count($params) === 1) { // Attempt to explode on a space if no ';' are found.
            $params = \explode(" ", $connectionString);
        }

        foreach ($params as $param) {
            list($key, $value) = \array_map("trim", \explode("=", $param, 2) + [1 => null]);

            if (isset(self::KEY_MAP[$key])) {
                $key = self::KEY_MAP[$key];
            }

            if (!\in_array($key, self::ALLOWED_KEYS, true)) {
                throw new \Error("Invalid key in connection string: " . $key);
            }

            $config->{$key} = $value;
        }

        if (!isset($config->host, $config->user, $config->pass)) {
            throw new \Error("Required parameters host, user and pass need to be passed in connection string");
        }

        $config->useCompression = $config->useCompression && $config->useCompression !== "false";

        $config->ssl = $sslOptions;

        return $config;
    }

    public function connectionString(): string
    {
        if ($this->resolvedHost !== null) {
            return $this->resolvedHost;
        }

        $index = \strpos($this->host, ':');

        if ($index === false) {
            return $this->resolvedHost = "tcp://{$this->host}:3306";
        }

        if ($index === 0) {
            return $this->resolvedHost = "tcp://localhost:" . (int) \substr($this->host, 1);
        }

        list($host, $port) = \explode(':', $this->host, 2);
        return $this->resolvedHost = "tcp://$host:" . (int) $port;
    }

    public function getHost(): string
    {
        return $this->host;
    }

    public function getUser(): string
    {
        return $this->user;
    }

    public function getPassword(): string
    {
        return $this->pass;
    }

    public function getDatabase()
    {
        return $this->db;
    }

    public function withDatabase(string $database): self
    {
        $new = clone $this;
        $new->db = $database;
        return $new;
    }

    public function isCompressionEnabled(): bool
    {
        return $this->useCompression;
    }

    public function withCompression()
    {
        $new = clone $this;
        $new->useCompression = true;
        return $new;
    }

    public function withoutCompression()
    {
        $new = clone $this;
        $new->useCompression = false;
        return $new;
    }

    public function getTlsContext()
    {
        return $this->ssl;
    }

    public function withTlsContext(ClientTlsContext $context)
    {
        $new = clone $this;
        $new->ssl = $context;
        return $new;
    }

    public function withoutTlsContext()
    {
        $new = clone $this;
        $new->ssl = null;
        return $new;
    }

    public function getCharset(): string
    {
        return $this->charset;
    }

    public function getCollation(): string
    {
        return $this->collate;
    }

    public function withCharset(string $charset, string $collate)
    {
        $new = clone $this;
        $new->charset = $charset;
        $new->collate = $collate;
        return $new;
    }

    public function getKey()
    {
        return $this->key;
    }

    public function withKey(string $key)
    {
        $new = clone $this;
        $new->key = $key;
        return $new;
    }
}
