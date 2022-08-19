<?php

namespace Amp\Mysql\Internal;

enum MysqlResultProxyState
{
    case Initial;
    case Fetched;
    case Complete;
}
