<?php

namespace Amp\Mysql\Internal;

use Amp\Struct;

final class ConnectionState
{
    use Struct;

    /** @var int|null */
    public $affectedRows;
    public $insertId;
    public $statusFlags;
    public $warnings;
    public $statusInfo;
    public $sessionState = [];
    public $errorMsg;
    public $errorCode;
    public $errorState; // begins with "#"

    /** @var string */
    public $serverVersion;

    /** @var string */
    public $charset;
}
