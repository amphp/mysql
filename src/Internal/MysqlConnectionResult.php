<?php

namespace Amp\Mysql\Internal;

use Amp\DeferredFuture;
use Amp\Future;
use Amp\Mysql\MysqlResult;
use Revolt\EventLoop;
use function Amp\async;

/** @internal */
final class MysqlConnectionResult implements MysqlResult, \IteratorAggregate
{
    private readonly MysqlResultProxy $result;

    private readonly \Generator $generator;

    private ?Future $nextResult = null;

    public function __construct(MysqlResultProxy $result)
    {
        $this->result = $result;
        $this->generator = self::iterate($result);
    }

    private static function iterate(MysqlResultProxy $result): \Generator
    {
        if (!$result->rowIterator->continue()) {
            return;
        }

        // Column names are only available once a result row has been fetched.
        $columnNames = \array_column($result->columns, 'name');

        do {
            $row = $result->rowIterator->getValue();
            yield \array_combine($columnNames, $row);
        } while ($result->rowIterator->continue());
    }

    public function __destruct()
    {
        EventLoop::queue(self::dispose(...), $this->generator);
    }

    private static function dispose(\Generator $generator): void
    {
        try {
            // Discard remaining rows in the result set.
            while ($generator->valid()) {
                $generator->next();
            }
        } catch (\Throwable) {
            // Ignore errors while discarding result.
        }
    }

    public function getIterator(): \Traversable
    {
        // Using a Generator to keep a reference to $this.
        yield from $this->generator;
    }

    public function getNextResult(): ?MysqlResult
    {
        $this->nextResult ??= async(function (): ?MysqlResult {
            $deferred = $this->result->next ??= new DeferredFuture;
            $result = $deferred->getFuture()->await();

            if ($result instanceof MysqlResultProxy) {
                return new self($result);
            }

            return $result; // Instance of CommandResult or null.
        });

        return $this->nextResult->await();
    }

    public function getRowCount(): ?int
    {
        return $this->result->affectedRows;
    }

    public function getColumnCount(): int
    {
        return $this->result->columnCount;
    }

    public function getLastInsertId(): ?int
    {
        return $this->result->insertId;
    }

    public function getColumnDefinitions(): ?array
    {
        return $this->result->getColumnDefinitions();
    }
}
