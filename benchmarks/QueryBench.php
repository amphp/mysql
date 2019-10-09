<?php

namespace Amp\Mysql\Bench;

use Amp\Mysql\CancellableConnector;
use Amp\Mysql\Connection;
use Amp\Mysql\ConnectionConfig;
use Amp\Mysql\Pool as ConnectionPool;
use Amp\Mysql\ResultSet;
use function Amp\call;
use function Amp\Promise\wait;

/**
 * @BeforeMethods({"init"})
 * @AfterMethods({"onAfterMethods"})
 * @Iterations(1)
 * @Revs(100)
 * @Warmup(1)
 * @OutputTimeUnit("milliseconds", precision=5)
 */
class QueryBench extends AbstractBench
{
    /** @var  ConnectionPool */
    protected $connectionPool;

    /** @var  Connection */
    protected $connection;

    /** @var  \PDO */
    protected $pdoConnection;

    /** @var int */
    protected $maxQueries = 10;

    /** @var int */
    protected $poolLimit = 10;

    public function init()
    {
        $config = ConnectionConfig::fromString("host=$this->host;user=$this->user;pass=$this->pass");
        $connector = new CancellableConnector;
        $this->connectionPool = new ConnectionPool($config, $this->poolLimit, 10, $connector);
        $connectionPromise = $connector->connect($config);
        $this->connection = wait($connectionPromise);
        $this->pdoConnection = new \PDO("mysql:host=$this->host;port=3306", $this->user, $this->pass);
    }

    public function onAfterMethods()
    {
        $this->connectionPool->close();
        $this->connection->close();
    }

    public function benchPdoQueries()
    {
        foreach (\range(1, $this->maxQueries) as $ii) {
            $resultSet = $this->pdoConnection->query("SELECT $ii");
            $resultSet->fetch(\PDO::FETCH_ASSOC);
        }
    }

    public function benchSyncQueries()
    {
        wait(call(function () {
            $connection = $this->connection;
            foreach (\range(1, $this->maxQueries) as $i) {
                /** @var ResultSet $resultSet */
                $resultSet = yield $connection->query("SELECT $i");
                yield $resultSet->advance();
            }
        }));
    }

    public function benchAsyncQueries()
    {
        wait(call(function () {
            $connection = $this->connection;
            /** @var ResultSet[] $resultSets */
            $resultSets = yield \array_map(function ($i) use ($connection) {
                return $connection->query("SELECT $i");
            }, \range(1, $this->maxQueries));
            yield \array_map(function ($resultSet) {
                /** @var ResultSet $resultSet */
                return $resultSet->advance();
            }, $resultSets);
        }));
    }

    public function benchAsyncQueriesUsingPool()
    {
        wait(call(function () {
            $connection = $this->connectionPool;
            /** @var ResultSet[] $resultSets */
            $resultSets = yield \array_map(function ($i) use ($connection) {
                return $connection->query("SELECT $i");
            }, \range(1, $this->maxQueries));
            yield \array_map(function ($resultSet) {
                /** @var ResultSet $resultSet */
                return $resultSet->advance();
            }, $resultSets);
        }));
    }
}
