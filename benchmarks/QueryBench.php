<?php

namespace Amp\Mysql\Bench;

use Amp\Future;
use Amp\Mysql\MysqlConnectionPool;
use Amp\Mysql\MysqlLink;
use Amp\Mysql\SocketMysqlConnector;
use Amp\Mysql\MysqlConnection;
use Amp\Mysql\MysqlConfig;
use PhpBench\Attributes\AfterMethods;
use PhpBench\Attributes\BeforeMethods;
use PhpBench\Attributes\Iterations;
use PhpBench\Attributes\Revs;
use PhpBench\Attributes\OutputTimeUnit;
use PhpBench\Attributes\Warmup;
use function Amp\async;

#[
    BeforeMethods('init'),
    AfterMethods('cleanup'),
    Iterations(1),
    Revs(100),
    Warmup(1),
    OutputTimeUnit('milliseconds', precision: 5),
]
class QueryBench extends AbstractBench
{
    protected MysqlConnectionPool $connectionPool;

    protected MysqlConnection $connection;

    protected \PDO $pdoConnection;

    protected int $maxQueries = 100;

    /** @var int */
    protected int $poolLimit = 10;

    public function init(): void
    {
        $config = MysqlConfig::fromAuthority($this->host, $this->user, $this->password);
        $connector = new SocketMysqlConnector;

        $this->connectionPool = new MysqlConnectionPool(
            config: $config,
            maxConnections: $this->poolLimit,
            connector: $connector,
        );

        $this->connection = $connector->connect($config);

        $this->pdoConnection = new \PDO("mysql:host=$this->host", $this->user, $this->password);
    }

    public function cleanup(): void
    {
        $this->connectionPool->close();
        $this->connection->close();
    }

    public function benchPdoQueries(): void
    {
        $statement = $this->pdoConnection->prepare("SELECT ?");

        foreach (\range(1, $this->maxQueries) as $i) {
            $statement->execute([$i]);
            $statement->fetch(\PDO::FETCH_ASSOC);
        }
    }

    public function benchSyncQueries(): void
    {
        $statement = $this->connection->prepare("SELECT ?");

        foreach (\range(1, $this->maxQueries) as $i) {
            \iterator_to_array($statement->execute([$i]));
        }
    }

    private function runAsyncQueries(MysqlLink $link): void
    {
        $statement = $link->prepare("SELECT ?");

        Future\await(\array_map(
            fn (int $i) => async(fn () => \iterator_to_array($statement->execute([$i]))),
            \range(1, $this->maxQueries),
        ));
    }

    public function benchAsyncQueriesUsingSingleConnection(): void
    {
        $this->runAsyncQueries($this->connection);
    }

    public function benchAsyncQueriesUsingConnectionPool(): void
    {
        $this->runAsyncQueries($this->connectionPool);
    }
}
