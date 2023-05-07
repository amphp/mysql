<?php declare(strict_types=1);

namespace Amp\Mysql\Test;

use Amp\Mysql\MysqlLink;
use Amp\Mysql\MysqlNestableTransaction;
use Amp\Mysql\MysqlTransaction;
use Amp\Mysql\SocketMysqlConnector;

class NestedTransactionTest extends LinkTest
{
    private ?MysqlLink $link;
    private ?MysqlTransaction $transaction = null;

    public function getLink(bool $useCompression = false): MysqlLink
    {
        $this->link = (new SocketMysqlConnector)->connect($this->getConfig($useCompression));
        $this->transaction = $this->link->beginTransaction();
        return new MysqlNestableTransaction($this->transaction);
    }

    public function tearDown(): void
    {
        if ($this->transaction?->isActive()) {
            $this->transaction->rollback();
        }

        $this->link?->close();

        parent::tearDown();
    }
}
