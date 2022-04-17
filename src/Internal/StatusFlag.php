<?php

namespace Amp\Mysql\Internal;

/** @see 14.1.3.4 Status Flags */
enum StatusFlag: int
{
    case InTransaction = 0x0001; // a transaction is active
    case AutoCommit = 0x0002; // auto-commit is enabled
    case MoreResultsExist = 0x0008;
    case NoGoodIndexUsed = 0x0010;
    case NoIndexUsed = 0x0020;
    case CursorExists = 0x0040; // Used by Binary Protocol Result to signal that COM_STMT_FETCH must be used to fetch row-data.
    case LastRowSent = 0x0080;
    case DbDropped = 0x0100;
    case NoBackslashEscapes = 0x0200;
    case MetadataChanged = 0x0400;
    case ServerQueryWasSlow = 0x0800;
    case PsOutParams = 0x1000;
    case InReadonlyTransaction = 0x2000; // in a read-only transaction
    case SessionStateChanged = 0x4000; // connection state information has changed

    public function inFlags(int $flags): bool
    {
        return (bool) ($this->value & $flags);
    }
}
