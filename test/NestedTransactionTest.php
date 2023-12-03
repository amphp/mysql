<?php declare(strict_types=1);

namespace Amp\Mysql\Test;

use Amp\Mysql\MysqlLink;
use Amp\Mysql\MysqlTransaction;
use Amp\Mysql\SocketMysqlConnector;

class NestedTransactionTest extends LinkTest
{
    private ?MysqlLink $link;
    private ?MysqlTransaction $transaction = null;
    private ?MysqlTransaction $nested = null;

    public function getLink(bool $useCompression = false): MysqlLink
    {
        $this->link = (new SocketMysqlConnector)->connect($this->getConfig($useCompression));
        $this->transaction = $this->link->beginTransaction();
        $this->nested = $this->transaction->beginTransaction();

        return $this->nested;
    }

    public function tearDown(): void
    {
        if ($this->nested?->isActive()) {
            $this->nested->rollback();
        }

        if ($this->transaction?->isActive()) {
            $this->transaction->rollback();
        }

        $this->link?->close();

        parent::tearDown();
    }
}
