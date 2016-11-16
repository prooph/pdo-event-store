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
use PHPUnit_Framework_TestCase as TestCase;
use Prooph\Common\Event\ProophActionEventEmitter;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\CanControlTransactionActionEventEmitterAware;
use Prooph\EventStore\PDO\IndexingStrategy\PostgresSimpleStreamStrategy;
use Prooph\EventStore\PDO\PostgresEventStore;
use Prooph\EventStore\PDO\TableNameGeneratorStrategy\Sha1;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;
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
        $this->connection->exec(file_get_contents(__DIR__ . '/../../scripts/postgres_event_streams_table.sql'));
        $this->connection->exec(file_get_contents(__DIR__ . '/../../scripts/postgres_projections_table.sql'));

        $this->eventStore = new PostgresEventStore(
            new ProophActionEventEmitter([
                CanControlTransactionActionEventEmitterAware::EVENT_APPEND_TO,
                CanControlTransactionActionEventEmitterAware::EVENT_CREATE,
                CanControlTransactionActionEventEmitterAware::EVENT_LOAD,
                CanControlTransactionActionEventEmitterAware::EVENT_LOAD_REVERSE,
                CanControlTransactionActionEventEmitterAware::EVENT_DELETE,
                CanControlTransactionActionEventEmitterAware::EVENT_BEGIN_TRANSACTION,
                CanControlTransactionActionEventEmitterAware::EVENT_COMMIT,
                CanControlTransactionActionEventEmitterAware::EVENT_ROLLBACK,
            ]),
            new FQCNMessageFactory(),
            new NoOpMessageConverter(),
            TestUtil::getConnection(),
            new PostgresSimpleStreamStrategy(),
            new Sha1()
        );
    }

    protected function tearDown(): void
    {
        $this->connection->exec('DROP TABLE event_streams;');
        $this->connection->exec('DROP TABLE projections;');
        $this->connection->exec('DROP TABLE _' . sha1('user-123'));
    }

    protected function prepareEventStream(string $name): void
    {
        $events = [];
        $events[] = UserCreated::with([
            'name' => 'Alex'
        ], 1);
        for ($i = 2; $i < 50; $i++) {
            $events[] = UsernameChanged::with([
                'name' => uniqid('name_')
            ], $i);
        }
        $events[] = UsernameChanged::with([
            'name' => 'Sascha'
        ], 50);

        $this->eventStore->create(new Stream(new StreamName($name), new ArrayIterator($events)));
    }
}
