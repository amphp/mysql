<?php declare(strict_types=1);

namespace Amp\Mysql\Test;

use Amp\Mysql\SocketMysqlConnector;

/**
 * Exercises the prepared-statement parameter encoding path against MariaDB's native UUID column type.
 * Regresses if VarString is not chosen for string-family targets (see #142).
 */
class MariaDbUuidTest extends MysqlTestCase
{
    private const FIXTURE_UUID = '550e8400-e29b-41d4-a716-446655440000';

    protected function setUp(): void
    {
        parent::setUp();

        $db = (new SocketMysqlConnector)->connect($this->getConfig());

        $version = '';
        foreach ($db->query('SELECT VERSION() AS v') as $row) {
            $version = (string) $row['v'];
        }

        if (!\str_contains($version, 'MariaDB')) {
            $db->close();
            self::markTestSkipped('Requires MariaDB (got: ' . $version . ')');
        }

        // Native UUID column type landed in MariaDB 10.7.
        if (\preg_match('/^(\d+)\.(\d+)/', $version, $matches) !== 1
            || \version_compare($matches[1] . '.' . $matches[2], '10.7', '<')) {
            $db->close();
            self::markTestSkipped('Requires MariaDB >= 10.7 for native UUID column (got: ' . $version . ')');
        }

        $db->query('DROP TABLE IF EXISTS uuid_test');
        $db->query('CREATE TABLE uuid_test (id UUID PRIMARY KEY, label VARCHAR(64))');
        $db->close();
    }

    protected function tearDown(): void
    {
        $db = (new SocketMysqlConnector)->connect($this->getConfig());
        $db->query('DROP TABLE IF EXISTS uuid_test');
        $db->close();

        parent::tearDown();
    }

    public function testPreparedInsertRoundTripsNativeUuid(): void
    {
        $db = (new SocketMysqlConnector)->connect($this->getConfig());

        $statement = $db->prepare('INSERT INTO uuid_test (id, label) VALUES (?, ?)');
        $statement->execute([self::FIXTURE_UUID, 'fixture']);

        $rows = [];
        foreach ($db->query('SELECT id, label FROM uuid_test') as $row) {
            $rows[] = $row;
        }

        $db->close();

        self::assertCount(1, $rows);
        self::assertSame(self::FIXTURE_UUID, $rows[0]['id']);
        self::assertSame('fixture', $rows[0]['label']);
    }
}
