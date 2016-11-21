<?php
/**
 * This file is part of the prooph/pdo-event-store.
 * (c) 2016-2016 prooph software GmbH <contact@prooph.de>
 * (c) 2016-2016 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace ProophTest\EventStore\PDO;

use PDO;
use Prooph\Common\Event\ProophActionEventEmitter;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\ActionEventEmitterAware;
use Prooph\EventStore\PDO\IndexingStrategy\MySQLSingleStreamStrategy;
use Prooph\EventStore\PDO\IndexingStrategy\MySQLAggregateStreamStrategy;
use Prooph\EventStore\PDO\MySQLEventStore;
use Prooph\EventStore\PDO\TableNameGeneratorStrategy\Sha1;
use Prooph\EventStore\Exception\ConcurrencyException;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\Mock\UsernameChanged;
use Ramsey\Uuid\Uuid;

/**
 * @group pdo_mysql
 */
final class MySQLEventStoreTest extends AbstractPDOEventStoreTest
{
    /**
     * @var MySQLEventStore
     */
    protected $eventStore;

    protected function setUp(): void
    {
        if (TestUtil::getDatabaseVendor() !== 'pdo_mysql') {
            throw new \RuntimeException('Invalid database vendor');
        }

        $this->connection = TestUtil::getConnection();
        $this->connection->exec(file_get_contents(__DIR__ . '/../scripts/mysql_event_streams_table.sql'));

        $this->eventStore = $this->createEventStore($this->connection);
    }

    protected function createEventStore(PDO $connection): MySQLEventStore
    {
        return new MySQLEventStore(
            new ProophActionEventEmitter([
                ActionEventEmitterAware::EVENT_APPEND_TO,
                ActionEventEmitterAware::EVENT_CREATE,
                ActionEventEmitterAware::EVENT_LOAD,
                ActionEventEmitterAware::EVENT_LOAD_REVERSE,
                ActionEventEmitterAware::EVENT_DELETE,
                ActionEventEmitterAware::EVENT_HAS_STREAM,
                ActionEventEmitterAware::EVENT_FETCH_STREAM_METADATA,
            ]),
            new FQCNMessageFactory(),
            new NoOpMessageConverter(),
            $connection,
            new MySQLAggregateStreamStrategy(),
            new Sha1()
        );
    }

    /**
     * @test
     */
    public function it_fails_to_write_with_duplicate_version_and_single_stream_per_aggregate_strategy(): void
    {
        $this->expectException(ConcurrencyException::class);

        $streamEvent = UserCreated::with(
            ['name' => 'Max Mustermann', 'email' => 'contact@prooph.de'],
            1
        );

        $aggregateId = Uuid::uuid4()->toString();

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');
        $streamEvent = $streamEvent->withAddedMetadata('_aggregate_id', $aggregateId);
        $streamEvent = $streamEvent->withAddedMetadata('_aggregate_type', 'user');

        $stream = new Stream(new StreamName('Prooph\Model\User'), new \ArrayIterator([$streamEvent]));

        $this->eventStore->create($stream);

        $streamEvent = UsernameChanged::with(
            ['name' => 'John Doe'],
            1
        );

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');
        $streamEvent = $streamEvent->withAddedMetadata('_aggregate_id', $aggregateId);
        $streamEvent = $streamEvent->withAddedMetadata('_aggregate_type', 'user');

        $this->eventStore->appendTo(new StreamName('Prooph\Model\User'), new \ArrayIterator([$streamEvent]));
    }

    /**
     * @test
     */
    public function it_fails_to_write_with_duplicate_version_and_aggregate_stream_strategy(): void
    {
        $this->expectException(ConcurrencyException::class);

        $this->eventStore = new MySQLEventStore(
            new ProophActionEventEmitter([
                ActionEventEmitterAware::EVENT_APPEND_TO,
                ActionEventEmitterAware::EVENT_CREATE,
                ActionEventEmitterAware::EVENT_LOAD,
                ActionEventEmitterAware::EVENT_LOAD_REVERSE,
                ActionEventEmitterAware::EVENT_DELETE,
                ActionEventEmitterAware::EVENT_HAS_STREAM,
                ActionEventEmitterAware::EVENT_FETCH_STREAM_METADATA,
            ]),
            new FQCNMessageFactory(),
            new NoOpMessageConverter(),
            $this->connection,
            new MySQLSingleStreamStrategy(),
            new Sha1()
        );

        $streamEvent = UserCreated::with(
            ['name' => 'Max Mustermann', 'email' => 'contact@prooph.de'],
            1
        );

        $aggregateId = Uuid::uuid4()->toString();

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');
        $streamEvent = $streamEvent->withAddedMetadata('_aggregate_id', $aggregateId);
        $streamEvent = $streamEvent->withAddedMetadata('_aggregate_type', 'user');

        $stream = new Stream(new StreamName('Prooph\Model\User'), new \ArrayIterator([$streamEvent]));

        $this->eventStore->create($stream);

        $streamEvent = UsernameChanged::with(
            ['name' => 'John Doe'],
            1
        );

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');
        $streamEvent = $streamEvent->withAddedMetadata('_aggregate_id', $aggregateId);
        $streamEvent = $streamEvent->withAddedMetadata('_aggregate_type', 'user');

        $this->eventStore->appendTo(new StreamName('Prooph\Model\User'), new \ArrayIterator([$streamEvent]));
    }
}
