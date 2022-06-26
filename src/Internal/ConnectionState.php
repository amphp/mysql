<?php

namespace Amp\Mysql\Internal;

/** @internal */
enum ConnectionState
{
    case Unconnected;
    case Established;
    case Ready;
    case Closing;
    case Closed;
}
