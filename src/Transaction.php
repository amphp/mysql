<?php

namespace Amp\Mysql;

use Amp\Sql\Transaction as SqlTransaction;

interface Transaction extends Executor, SqlTransaction
{
}
