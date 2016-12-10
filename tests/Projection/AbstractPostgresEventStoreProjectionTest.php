<?php
/**
 * This file is part of the prooph/pdo-event-store.
 * (c) 2016-2016 prooph software GmbH <contact@prooph.de>
 * (c) 2016-2016 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace ProophTest\EventStore\PDO\Projection;

use ArrayIterator;
use PDO;
use PHPUnit\Framework\TestCase;
use Prooph\Common\Event\ProophActionEventEmitter;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\PDO\PersistenceStrategy\PostgresSimpleStreamStrategy;
use Prooph\EventStore\PDO\PostgresEventStore;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;
use Prooph\EventStore\TransactionalActionEventEmitterEventStore;
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\Mock\UsernameChanged;
use ProophTest\EventStore\PDO\TestUtil;

abstract class AbstractPostgresEventStoreProjectionTest extends TestCase
{
    /**
     * @var PostgresEventStore
     */
    protected $eventStore;

    /**
     * @var PDO
     */
    protected $connection;

    protected function setUp(): void
    {
        if (TestUtil::getDatabaseVendor() !== 'pdo_pgsql') {
            throw new \RuntimeException('Invalid database vendor');
        }

        $this->connection = TestUtil::getConnection();
        $this->connection->exec(file_get_contents(__DIR__.'/../../scripts/postgres/01_event_streams_table.sql'));
        $this->connection->exec(file_get_contents(__DIR__.'/../../scripts/postgres/02_projections_table.sql'));

        $this->eventStore = new PostgresEventStore(
            new ProophActionEventEmitter([
                TransactionalActionEventEmitterEventStore::EVENT_APPEND_TO,
                TransactionalActionEventEmitterEventStore::EVENT_CREATE,
                TransactionalActionEventEmitterEventStore::EVENT_LOAD,
                TransactionalActionEventEmitterEventStore::EVENT_LOAD_REVERSE,
                TransactionalActionEventEmitterEventStore::EVENT_DELETE,
                TransactionalActionEventEmitterEventStore::EVENT_HAS_STREAM,
                TransactionalActionEventEmitterEventStore::EVENT_FETCH_STREAM_METADATA,
                TransactionalActionEventEmitterEventStore::EVENT_UPDATE_STREAM_METADATA,
                TransactionalActionEventEmitterEventStore::EVENT_BEGIN_TRANSACTION,
                TransactionalActionEventEmitterEventStore::EVENT_COMMIT,
                TransactionalActionEventEmitterEventStore::EVENT_ROLLBACK,
            ]),
            new FQCNMessageFactory(),
            new NoOpMessageConverter(),
            TestUtil::getConnection(),
            new PostgresSimpleStreamStrategy()
        );
    }

    protected function tearDown(): void
    {
        // these tables are used in every test case
        $this->connection->exec('DROP TABLE event_streams;');
        $this->connection->exec('DROP TABLE projections;');
        $this->connection->exec('DROP TABLE _' . sha1('user-123'));
        // these tables are used only in some test cases
        $this->connection->exec('DROP TABLE IF EXISTS _' . sha1('user-234'));
        $this->connection->exec('DROP TABLE IF EXISTS _' . sha1('$iternal-345'));
        $this->connection->exec('DROP TABLE IF EXISTS _' . sha1('guest-345'));
        $this->connection->exec('DROP TABLE IF EXISTS _' . sha1('guest-456'));
        $this->connection->exec('DROP TABLE IF EXISTS _' . sha1('foo'));
        $this->connection->exec('DROP TABLE IF EXISTS _' . sha1('test_projection'));
    }

    protected function prepareEventStream(string $name): void
    {
        $events = [];
        $events[] = UserCreated::with([
            'name' => 'Alex',
        ], 1);
        for ($i = 2; $i < 50; $i++) {
            $events[] = UsernameChanged::with([
                'name' => uniqid('name_'),
            ], $i);
        }
        $events[] = UsernameChanged::with([
            'name' => 'Sascha',
        ], 50);

        $this->eventStore->create(new Stream(new StreamName($name), new ArrayIterator($events)));
    }
}
