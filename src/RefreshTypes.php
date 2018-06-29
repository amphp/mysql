<?php

namespace Amp\Mysql;

final class RefreshTypes
{
    const REFRESH_GRANT = 0x01;
    const REFRESH_LOG = 0x02;
    const REFRESH_TABLES = 0x04;
    const REFRESH_HOSTS = 0x08;
    const REFRESH_STATUS = 0x10;
    const REFRESH_THREADS = 0x20;
    const REFRESH_SLAVE = 0x40;
    const REFRESH_MASTER = 0x80;
}
