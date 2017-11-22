<?php

namespace Amp\Mysql;

use Amp\Struct;

class ConnectionState {
    use Struct;

    public $affectedRows;
    public $insertId;
    public $statusFlags;
    public $warnings;
    public $statusInfo;
    public $sessionState = [];
    public $errorMsg;
    public $errorCode;
    public $errorState; // begins with "#"
    public $serverVersion;
    public $charset;
}
