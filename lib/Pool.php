<?php

namespace Amp\Mysql;

use Amp\Promise;

class Pool {
    private $connectionPool;

    public function __construct($connStr, $sslOptions = null) {
        if (preg_match("((?:^|;)\s*limit\s*=\s*([^;]*?)\s*(?:;|$))is", $connStr, $match, PREG_OFFSET_CAPTURE)) {
            $limit = (int) $match[1][0];
            $connStr = substr_replace($connStr, ";", $match[0][1], strlen($match[0][0]));
        } else {
            $limit = INF;
        }

        $config = Connection::parseConnStr($connStr, $sslOptions);
        $this->connectionPool = new ConnectionPool($config, $limit);
    }

    public function init() {
        return $this->connectionPool->getConnectionPromise();
    }

    public function setCharset(string $charset, string $collate = "") {
        $this->connectionPool->setCharset($charset, $collate);
    }

    public function query(string $query): Promise {
        return $this->connectionPool->getReadyConnection()->query($query);
    }

    public function listFields(string $table, string $like = "%"): Promise {
        return $this->connectionPool->getReadyConnection()->listFields($table, $like);
    }

    public function listAllFields(string $table, string $like = "%"): Promise {
        return $this->connectionPool->getReadyConnection()->listAllFields($table, $like);
    }

    public function createDatabase(string $db): Promise {
        return $this->connectionPool->getReadyConnection()->createDatabase($db);
    }

    public function dropDatabase(string $db): Promise {
        return $this->connectionPool->getReadyConnection()->dropDatabase($db);
    }

    public function refresh(string $subcommand): Promise {
        return $this->connectionPool->getReadyConnection()->refresh($subcommand);
    }

    public function shutdown(): Promise {
        return $this->connectionPool->getReadyConnection()->shutdown();
    }

    public function statistics(): Promise {
        return $this->connectionPool->getReadyConnection()->statistics();
    }

    public function processInfo(): Promise {
        return $this->connectionPool->getReadyConnection()->processInfo();
    }

    public function killProcess($process): Promise {
        return $this->connectionPool->getReadyConnection()->killProcess($process);
    }

    public function debugStdout(): Promise {
        return $this->connectionPool->getReadyConnection()->debugStdout();
    }

    public function ping(): Promise {
        return $this->connectionPool->getReadyConnection()->ping();
    }

    public function prepare(string $query, $data = null): Promise {
        return $this->connectionPool->getReadyConnection()->prepare($query, $data);
    }

    /* extracts a Connection and returns it, wrapped in a Promise */
    public function getConnection(): Promise {
        return $this->connectionPool->extractConnection();
    }

    public function close() {
        $this->connectionPool->close();
    }

    public function __destruct() {
        $this->close();
    }
}
