<?php declare(strict_types=1);

namespace Amp\Mysql\Test;

use Amp\Mysql\MysqlConnection;
use Amp\Mysql\SocketMysqlConnector;

/**
 * Exercises the prepared-statement parameter encoding path against MariaDB's native UUID column type.
 * Regresses if VarString is not chosen for string-family targets (see #142).
 */
class MariaDbUuidTest extends MysqlTestCase
{
    private const FIXTURE_UUID = '550e8400-e29b-41d4-a716-446655440000';

    private MysqlConnection $db;

    protected function setUp(): void
    {
        parent::setUp();

        $version = $this->getDbVersion();

        // Native UUID column type landed in MariaDB 10.7.
        if (\preg_match('/^(\d+)\.(\d+)/', $version, $matches) !== 1
            || \version_compare($matches[1] . '.' . $matches[2], '10.7', '<')
        ) {
            self::markTestSkipped('Requires MariaDB >= 10.7 for native UUID column (got: ' . $version . ')');
        }

        $this->db = (new SocketMysqlConnector())->connect($this->getConfig());
        $this->db->query('DROP TABLE IF EXISTS uuid_test');
        $this->db->query('CREATE TABLE uuid_test (id UUID PRIMARY KEY, label VARCHAR(64))');
    }

    protected function tearDown(): void
    {
        $this->db->query('DROP TABLE IF EXISTS uuid_test');
        $this->db->close();

        parent::tearDown();
    }

    public function testPreparedInsertRoundTripsNativeUuid(): void
    {
        $statement = $this->db->prepare('INSERT INTO uuid_test (id, label) VALUES (?, ?)');
        $statement->execute([self::FIXTURE_UUID, 'fixture']);

        $rows = [];
        foreach ($this->db->query('SELECT id, label FROM uuid_test') as $row) {
            $rows[] = $row;
        }

        self::assertCount(1, $rows);
        self::assertSame(self::FIXTURE_UUID, $rows[0]['id']);
        self::assertSame('fixture', $rows[0]['label']);
    }
}
