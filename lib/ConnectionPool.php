<?php

namespace Amp\Mysql;

class ConnectionPool {
    private $connections = [];
    private $connectionMap = [];
    private $ready = [];
    private $readyMap = [];
    private $connectionDeferred;
    private $virtualConnection;
    private $config;
    private $limit;

    public function __construct($config, $limit) {
        $config->ready = function($hash) {
            $this->ready($hash);
        };
        /* @TODO ... pending queries ... (!!) */
        $config->restore = function($hash, $init) {
            $this->unmapConnection($hash);
            if ($init && empty($this->connections)) {
                $this->virtualConnection->fail(new \Exception("Connection failed"));
            }
            return $this->getReadyConnection();
        };
        $config->busy = function($hash) {
            if (isset($this->readyMap[$hash])) {
                unset($this->ready[$this->readyMap[$hash]]);
            }
        };

        $this->config = $config;
        $this->limit = $limit;
        $this->virtualConnection = new VirtualConnection;

        $this->addConnection();
    }

    public function getConnectionPromise() {
        if (isset($this->connectionDeferred)) {
            return $this->connectionDeferred->promise();
        }
        if ($this->connections) {
            foreach ($this->connections as $conn) {
                if ($conn->isReady()) {
                    return new \Amp\Success;
                }
            }
        }
        $this->connectionDeferred = new \Amp\Deferred;
        return $this->connectionDeferred->promise();
    }

    public function addConnection() {
        if (count($this->connections) >= $this->limit) {
            return;
        }

        $this->connections[] = $conn = new Connection($this->config);
        end($this->connections);
        $this->connectionMap[spl_object_hash($conn)] = key($this->connections);
        $conn->connect()->onResolve(function ($error) use ($conn) {
            if (!$error && !$conn->isReady()) {
                $error = new InitializationException("Connection closed before being established");
            }
            if ($error) {
                $this->unmapConnection(spl_object_hash($conn));
                if (empty($this->connections)) {
                    $this->virtualConnection->fail($error);
                    $deferred = $this->connectionDeferred;
                    if ($deferred) {
                        $this->connectionDeferred = null;
                        $deferred->fail($error);
                    }
                }
                return;
            }

            if (isset($this->connectionDeferred)) {
                $deferred = $this->connectionDeferred;
                $this->connectionDeferred = null;
                $deferred->resolve();
            }

            if ($this->config->charset != "utf8mb4" || ($this->config->collate != "" && $this->config->collate != "utf8mb4_general_ci")) {
                $conn->setCharset($this->config->charset, $this->config->collate);
            }
        });
    }

    /** First parameter may be collation too, then charset is determined by the prefix of collation */
    public function setCharset($charset, $collate = "") {
        if ($collate === "" && false !== $off = strpos($charset, "_")) {
            $collate = $charset;
            $charset = substr($collate, 0, $off);
        }

        $this->ready = [];
        $this->config->charset = $charset;
        $this->config->collate = $collate;
        foreach ($this->connections as $conn) {
            if ($conn->alive()) {
                $conn->setCharset($charset, $collate);
            }
        }
    }

    public function useExceptions($set) {
        $this->config->exceptions = $set;
    }

    private function ready($hash) {
        $conn = $this->connections[$this->connectionMap[$hash]];
        if (list($deferred, $method, $args) = $this->virtualConnection->getCall()) {
            $deferred->resolve(call_user_func_array([$conn, $method], $args));
        } else {
            $this->ready[] = $conn;
            end($this->ready);
            $this->readyMap[$hash] = key($this->ready);
            reset($this->ready);
        }
    }

    /** @return Connection|VirtualConnection */
    public function getReadyConnection() {
        if ($this->limit < 0) {
            $this->limit *= -1;
        }
        if (count($this->ready) < 2) {
            $this->addConnection();
        }

        foreach ($this->ready as $key => &$conn) {
            unset($this->ready[$key]);
            if ($conn->isReady()) {
                return $conn;
            }
        }

        $this->addConnection();

        return $this->virtualConnection;
    }

    public function extractConnection() {
        return $this->getReadyConnection()->getThis()->onResolve(function($e, $conn) {
            $this->unmapConnection(spl_object_hash($conn));
        });
    }

    /* This method might be called multiple times with the same hash. Important is that it's unmapped immediately */
    private function unmapConnection($hash) {
        if (isset($this->connectionMap[$hash])) {
            unset($this->connections[$this->connectionMap[$hash]], $this->connectionMap[$hash]);
        }
    }

    public function __destruct() {
        $this->close();
    }

    public function close() {
        foreach ($this->connections as $conn) {
            $conn->forceClose();
            $this->unmapConnection(spl_object_hash($conn));
        }
        $this->ready = [];
        $this->readyMap = [];
        $this->limit *= -1;
    }
}