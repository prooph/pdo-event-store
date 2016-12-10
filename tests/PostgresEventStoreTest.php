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

namespace ProophTest\EventStore\PDO;

use PDO;
use Prooph\Common\Event\ProophActionEventEmitter;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\Exception\ConcurrencyException;
use Prooph\EventStore\Exception\StreamNotFound;
use Prooph\EventStore\Exception\TransactionAlreadyStarted;
use Prooph\EventStore\Metadata\MetadataMatcher;
use Prooph\EventStore\Metadata\Operator;
use Prooph\EventStore\PDO\Exception\RuntimeException;
use Prooph\EventStore\PDO\PersistenceStrategy\PostgresAggregateStreamStrategy;
use Prooph\EventStore\PDO\PersistenceStrategy\PostgresSingleStreamStrategy;
use Prooph\EventStore\PDO\PostgresEventStore;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;
use Prooph\EventStore\TransactionalActionEventEmitterEventStore;
use ProophTest\EventStore\Mock\TestDomainEvent;
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\Mock\UsernameChanged;
use Ramsey\Uuid\Uuid;

/**
 * @group pdo_pgsql
 */
final class PostgresEventStoreTest extends AbstractPDOEventStoreTest
{
    /**
     * @var PostgresEventStore
     */
    protected $eventStore;

    protected function setUp(): void
    {
        if (TestUtil::getDatabaseVendor() !== 'pdo_pgsql') {
            throw new \RuntimeException('Invalid database vendor');
        }

        $this->connection = TestUtil::getConnection();
        $this->connection->exec(file_get_contents(__DIR__.'/../scripts/postgres/01_event_streams_table.sql'));

        $this->eventStore = $this->createEventStore($this->connection);
    }

    protected function tearDown(): void
    {
        $this->connection->exec('DROP TABLE event_streams;');
        $this->connection->exec('DROP TABLE _' . sha1('Prooph\Model\User'));
    }

    protected function createEventStore(PDO $connection): PostgresEventStore
    {
        return new PostgresEventStore(
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
            $connection,
            new PostgresAggregateStreamStrategy()
        );
    }

    /**
     * @test
     */
    public function it_fails_to_write_with_duplicate_version_and_aggregate_stream_strategy(): void
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
    public function it_fails_to_write_with_duplicate_version_and_mulitple_streams_per_aggregate_strategy(): void
    {
        $this->expectException(ConcurrencyException::class);

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
            $this->connection,
            new PostgresSingleStreamStrategy()
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

    /**
     * @test
     */
    public function it_can_rollback_transaction(): void
    {
        $this->expectException(StreamNotFound::class);

        $testStream = $this->getTestStream();

        $this->eventStore->beginTransaction();

        $this->eventStore->create($testStream);

        $this->eventStore->rollback();

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch('tag', Operator::EQUALS(), 'person');
        $stream = $this->eventStore->load(new StreamName('Prooph\Model\User'), 1, null, $metadataMatcher);

        $stream->streamEvents()->valid();
    }

    /**
     * @test
     */
    public function it_throws_exception_when_second_transaction_started(): void
    {
        $this->expectException(TransactionAlreadyStarted::class);

        $this->eventStore->beginTransaction();
        $this->eventStore->beginTransaction();
    }

    /**
     * @test
     */
    public function it_can_commit_empty_transaction(): void
    {
        $this->eventStore->beginTransaction();
        $this->eventStore->commit();
    }

    /**
     * @test
     */
    public function it_can_rollback_empty_transaction(): void
    {
        $this->eventStore->beginTransaction();
        $this->eventStore->rollback();
    }

    /**
     * @test
     */
    public function it_throws_exception_using_aggregate_stream_strategy_if_aggregate_version_is_missing_in_metadata(): void
    {
        $this->expectException(RuntimeException::class);

        $event = TestDomainEvent::fromArray([
            'uuid' => Uuid::uuid4()->toString(),
            'message_name' => 'test-message',
            'created_at' => new \DateTimeImmutable('now', new \DateTimeZone('UTC')),
            'payload' => [],
            'metadata' => [],
        ]);

        $stream = new Stream(new StreamName('Prooph\Model\User'), new \ArrayIterator([$event]));

        $this->eventStore->create($stream);
    }
}
