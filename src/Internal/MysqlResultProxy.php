<?php

namespace Amp\Mysql\Internal;

use Amp\DeferredFuture;
use Amp\Mysql\MysqlColumnDefinition;
use Amp\Pipeline\ConcurrentIterator;
use Amp\Pipeline\Queue;

/** @internal */
final class MysqlResultProxy
{
    public int $columnsToFetch = 0;

    /** @var list<MysqlColumnDefinition> */
    public array $columns = [];

    public array $params = [];

    private readonly Queue $rowQueue;

    public readonly ConcurrentIterator $rowIterator;

    private ?DeferredFuture $columnDeferred = null;

    private MysqlResultProxyState $state = MysqlResultProxyState::Initial;

    public ?DeferredFuture $next = null;

    public function __construct(
        public readonly int $columnCount = 0,
        ?int $columnsToFetch = null,
        public readonly ?int $affectedRows = null,
        public readonly ?int $insertId = null,
    ) {
        $this->rowQueue = new Queue();
        $this->rowIterator = $this->rowQueue->iterate();

        $this->columnsToFetch = $columnsToFetch ?? $this->columnCount;
    }

    public function getColumnDefinitions(): array
    {
        if ($this->state === MysqlResultProxyState::Initial) {
            $this->columnDeferred ??= new DeferredFuture();
            $this->columnDeferred->getFuture()->await();
        }

        return $this->columns;
    }

    public function updateState(MysqlResultProxyState $state): void
    {
        if ($this->state === MysqlResultProxyState::Complete) {
            throw new \RuntimeException('Result set already complete');
        }

        match ($state) {
            MysqlResultProxyState::Complete => $this->rowQueue->complete(),
            MysqlResultProxyState::ColumnsFetched => $this->columnsFetched(),
            MysqlResultProxyState::Initial => throw new \RuntimeException('Cannot reset to initial state'),
        };

        $this->state = $state;
    }

    private function columnsFetched(): void
    {
        $this->columnDeferred?->complete();
        $this->columnDeferred = null;
    }

    public function rowFetched(array $row): void
    {
        $this->rowQueue->push($row);
    }

    public function error(\Throwable $e): void
    {
        if ($this->state === MysqlResultProxyState::Complete) {
            return;
        }

        $this->state = MysqlResultProxyState::Complete;

        $this->rowQueue->error($e);

        $this->columnDeferred?->error($e);
        $this->columnDeferred = null;
    }
}
