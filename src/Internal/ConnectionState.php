<?php

namespace Amp\Mysql\Internal;

use Amp\Struct;

final class ConnectionState
{
    use Struct;

    public ?int $affectedRows = null;
    public ?int $insertId = null;
    public ?int $statusFlags = null;
    public ?int $warnings = null;
    public ?string $statusInfo = null;
    public array $sessionState = [];
    public ?string $errorMsg = null;
    public ?int $errorCode = null;
    public ?string $errorState = null; // begins with "#"

    public string $serverVersion;

    public int $charset;
}
