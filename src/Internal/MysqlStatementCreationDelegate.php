<?php declare(strict_types=1);

namespace Amp\Mysql\Internal;

use Amp\Mysql\MysqlStatement;
use Amp\Sql\SqlStatement;

/** @internal */
trait MysqlStatementCreationDelegate
{
    #[\Override]
    protected function createStatement(
        SqlStatement $statement,
        \Closure $release,
        ?\Closure $awaitBusyResource = null,
    ): MysqlStatement {
        \assert($statement instanceof MysqlStatement);
        return new MysqlPooledStatement($statement, $release, $awaitBusyResource);
    }
}
