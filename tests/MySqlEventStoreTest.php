<?php
/**
 * This file is part of the prooph/pdo-event-store.
 * (c) 2016-2018 prooph software GmbH <contact@prooph.de>
 * (c) 2016-2018 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace ProophTest\EventStore\Pdo;

use ArrayIterator;
use PDO;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\Exception\ConcurrencyException;
use Prooph\EventStore\Metadata\FieldType;
use Prooph\EventStore\Metadata\MetadataMatcher;
use Prooph\EventStore\Metadata\Operator;
use Prooph\EventStore\Pdo\Exception\JsonException;
use Prooph\EventStore\Pdo\Exception\RuntimeException;
use Prooph\EventStore\Pdo\MySqlEventStore;
use Prooph\EventStore\Pdo\PersistenceStrategy;
use Prooph\EventStore\Pdo\PersistenceStrategy\MySqlAggregateStreamStrategy;
use Prooph\EventStore\Pdo\PersistenceStrategy\MySqlSingleStreamStrategy;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\Mock\UsernameChanged;
use Ramsey\Uuid\Uuid;

/**
 * @group mysql
 */
class MySqlEventStoreTest extends AbstractPdoEventStoreTest
{
    /**
     * @var MySqlEventStore
     */
    protected $eventStore;

    protected function setUp(): void
    {
        if (TestUtil::getDatabaseDriver() !== 'pdo_mysql') {
            throw new \RuntimeException('Invalid database driver');
        }

        $this->connection = TestUtil::getConnection();
        TestUtil::initDefaultDatabaseTables($this->connection);

        $this->setupEventStoreWith(new MySqlAggregateStreamStrategy(new NoOpMessageConverter()));
    }

    /**
     * @test
     */
    public function it_cannot_create_new_stream_if_table_name_is_already_used(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Error during createSchemaFor');

        $streamName = new StreamName('foo');
        $schema = $this->persistenceStrategy->createSchema($this->persistenceStrategy->generateTableName($streamName));

        foreach ($schema as $command) {
            $statement = $this->connection->prepare($command);
            $statement->execute();
        }

        $this->eventStore->create(new Stream($streamName, new \ArrayIterator()));
    }

    /**
     * @test
     */
    public function it_loads_correctly_using_single_stream_per_aggregate_type_strategy(): void
    {
        $this->setupEventStoreWith(new MySqlSingleStreamStrategy(new NoOpMessageConverter()), 5);

        $streamName = new StreamName('Prooph\Model\User');

        $stream = new Stream($streamName, new ArrayIterator($this->getMultipleTestEvents()));

        $this->eventStore->create($stream);

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch('_aggregate_id', Operator::EQUALS(), 'one');
        $events = iterator_to_array($this->eventStore->load($streamName, 1, null, $metadataMatcher));
        $this->assertCount(100, $events);
        $lastUser1Event = array_pop($events);

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch('_aggregate_id', Operator::EQUALS(), 'two');
        $events = iterator_to_array($this->eventStore->load($streamName, 1, null, $metadataMatcher));
        $this->assertCount(100, $events);
        $lastUser2Event = array_pop($events);

        $this->assertEquals('Sandro', $lastUser1Event->payload()['name']);
        $this->assertEquals('Bradley', $lastUser2Event->payload()['name']);
    }

    /**
     * @test
     */
    public function it_loads_correctly_using_single_stream(): void
    {
        $batchMaxSize = 2;
        $this->setupEventStoreWith(new MySqlSingleStreamStrategy(new NoOpMessageConverter()), $batchMaxSize);

        $streamName = new StreamName('Prooph\Model\User');

        $stream = new Stream($streamName, new ArrayIterator($this->getMultipleTestEvents()));

        $this->eventStore->create($stream);

        $metadataMatcher = new MetadataMatcher();
        $iterator = $this->eventStore->load($streamName, 1, 5, $metadataMatcher);
        foreach ($iterator as $_) {
            $this->assertLessThanOrEqual($batchMaxSize, $iterator->count());
        }
    }

    /**
     * @test
     */
    public function it_fails_to_write_with_duplicate_version_and_mulitple_streams_per_aggregate_strategy(): void
    {
        $this->expectException(ConcurrencyException::class);

        $this->setupEventStoreWith(new MySqlSingleStreamStrategy(new NoOpMessageConverter()));

        $streamEvent = UserCreated::with(
            ['name' => 'Max Mustermann', 'email' => 'contact@prooph.de'],
            1
        );

        $aggregateId = Uuid::uuid4()->toString();

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');
        $streamEvent = $streamEvent->withAddedMetadata('_aggregate_id', $aggregateId);
        $streamEvent = $streamEvent->withAddedMetadata('_aggregate_type', 'user');

        $stream = new Stream(new StreamName('Prooph\Model\User'), new ArrayIterator([$streamEvent]));

        $this->eventStore->create($stream);

        $streamEvent = UsernameChanged::with(
            ['name' => 'John Doe'],
            1
        );

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');
        $streamEvent = $streamEvent->withAddedMetadata('_aggregate_id', $aggregateId);
        $streamEvent = $streamEvent->withAddedMetadata('_aggregate_type', 'user');

        $this->eventStore->appendTo(new StreamName('Prooph\Model\User'), new ArrayIterator([$streamEvent]));
    }

    public function it_ignores_transaction_handling_if_flag_is_enabled(): void
    {
        $connection = $this->prophesize(PDO::class);
        $connection->beginTransaction()->shouldNotBeCalled();
        $connection->commit()->shouldNotBeCalled();
        $connection->rollback()->shouldNotBeCalled();

        $eventStore = new MySqlEventStore(new FQCNMessageFactory(), $connection->reveal(), new MySqlAggregateStreamStrategy(new NoOpMessageConverter()));

        $streamEvent = UserCreated::with(
            ['name' => 'Max Mustermann', 'email' => 'contact@prooph.de'],
            1
        );

        $stream = new Stream(new StreamName('Prooph\Model\User'), new ArrayIterator([$streamEvent]));

        $eventStore->create($stream);

        $streamEvent = UsernameChanged::with(
            ['name' => 'John Doe'],
            1
        );

        $eventStore->appendTo(new StreamName('Prooph\Model\User'), new ArrayIterator([$streamEvent]));
    }

    /**
     * @test
     */
    public function it_removes_stream_if_stream_table_hasnt_been_created(): void
    {
        $strategy = $this->createMock(PersistenceStrategy::class);
        $strategy->method('createSchema')->willReturn(["SIGNAL SQLSTATE '45000';"]);
        $strategy->method('generateTableName')->willReturn('_non_existing_table');

        $this->setupEventStoreWith($strategy);

        $stream = new Stream(new StreamName('Prooph\Model\User'), new ArrayIterator());

        try {
            $this->eventStore->create($stream);
        } catch (RuntimeException $e) {
        }

        $this->assertFalse($this->eventStore->hasStream($stream->streamName()));
    }

    /**
     * @test
     * issue: https://github.com/prooph/pdo-event-store/issues/110
     */
    public function it_handles_invalid_json(): void
    {
        $this->expectException(JsonException::class);

        $event = UserCreated::with(['name' => ['John', 'ßnow']], 1);
        $event = $event->withAddedMetadata('key', 'value');

        $streamName = new StreamName('Prooph\Model\User');
        $stream = new Stream($streamName, new ArrayIterator([$event]), ['some' => ['metadata', 'as', 'well']]);

        $this->eventStore->create($stream);

        // Trigger an error when using an umlaut in the payload.
        $this->connection->query('SET NAMES latin1');

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            'event_id', Operator::EQUALS(), $event->uuid()->toString(), FieldType::MESSAGE_PROPERTY()
        );

        $streamEvents = $this->eventStore->load($streamName, 1, null, $metadataMatcher);
        $streamEvents->current(); // Trigger PdoStreamIterator.
    }
}
