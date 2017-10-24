<?php

namespace Amp\Mysql;

class ConnectionConfig {
    /* <domain/IP-string>(:<port>) */
    public $host;
    /* automatically resolved by Connection class */
    public $resolvedHost;
    public $user;
    public $pass;
    public $db = null;

    public $useCompression = false;

    /** @var array|null Null for no ssl, array with eventual ssl context options (peer_name is automatically set) */
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
}
