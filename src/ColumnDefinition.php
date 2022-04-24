<?php

namespace Amp\Mysql;

final class ColumnDefinition
{
    public function __construct(
        public readonly string $table,
        public readonly string $name,
        public readonly int $length,
        public readonly DataType $type,
        public readonly int $flags,
        public readonly int $decimals,
        public readonly string $defaults = '',
        public readonly ?string $originalTable = null,
        public readonly ?string $originalName = null,
        public readonly ?int $charset = null,
        public readonly ?string $catalog = null,
        public readonly ?string $schema = null,
    ) {
    }
}
