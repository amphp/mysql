<?php

namespace Amp\Mysql\Internal;

enum MysqlResultProxyState
{
    case Initial;
    case ColumnsFetched;
    case Complete;
}
