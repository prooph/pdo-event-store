<?php

/**
 * This file is part of prooph/pdo-event-store.
 * (c) 2016-2022 Alexander Miertsch <kontakt@codeliner.ws>
 * (c) 2016-2022 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace ProophTest\EventStore\Pdo;

use ArrayIterator;
use PDO;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\EventStore\Exception\ConcurrencyException;
use Prooph\EventStore\Metadata\MetadataMatcher;
use Prooph\EventStore\Metadata\Operator;
use Prooph\EventStore\Pdo\Exception\RuntimeException;
use Prooph\EventStore\Pdo\MariaDbEventStore;
use Prooph\EventStore\Pdo\PersistenceStrategy\MariaDbPersistenceStrategy;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\Mock\UsernameChanged;
use ProophTest\EventStore\Pdo\Assets\PersistenceStrategy\CustomMariaDbAggregateStreamStrategy;
use ProophTest\EventStore\Pdo\Assets\PersistenceStrategy\CustomMariaDbSingleStreamStrategy;
use Prophecy\PhpUnit\ProphecyTrait;
use Ramsey\Uuid\Uuid;

/**
 * @group mariadb
 */
final class MariaDbCustomStrategiesEventStoreTest extends MariaDbEventStoreTest
{
    use ProphecyTrait;

    /**
     * @var MariaDbEventStore
     */
    protected $eventStore;

    protected function setUp(): void
    {
        if (TestUtil::getDatabaseDriver() !== 'pdo_mysql') {
            throw new \RuntimeException('Invalid database driver');
        }

        $this->connection = TestUtil::getConnection();
        TestUtil::initDefaultDatabaseTables($this->connection);

        $this->setupEventStoreWith(new CustomMariaDbAggregateStreamStrategy());
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
        $this->setupEventStoreWith(new CustomMariaDbSingleStreamStrategy(), 5);

        $streamName = new StreamName('Prooph\Model\User');

        $stream = new Stream($streamName, new ArrayIterator($this->getMultipleTestEvents()));

        $this->eventStore->create($stream);

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch('_aggregate_id', Operator::EQUALS(), 'one');
        $events = \iterator_to_array($this->eventStore->load($streamName, 1, null, $metadataMatcher));
        $this->assertCount(100, $events);
        $lastUser1Event = \array_pop($events);

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch('_aggregate_id', Operator::EQUALS(), 'two');
        $events = \iterator_to_array($this->eventStore->load($streamName, 1, null, $metadataMatcher));
        $this->assertCount(100, $events);
        $lastUser2Event = \array_pop($events);

        $this->assertEquals('Sandro', $lastUser1Event->payload()['name']);
        $this->assertEquals('Bradley', $lastUser2Event->payload()['name']);
    }

    /**
     * @test
     */
    public function it_fails_to_write_with_duplicate_version_and_mulitple_streams_per_aggregate_strategy(): void
    {
        $this->expectException(ConcurrencyException::class);

        $this->setupEventStoreWith(new CustomMariaDbSingleStreamStrategy());

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

        $eventStore = new MariaDbEventStore(new FQCNMessageFactory(), $connection->reveal(), new CustomMariaDbAggregateStreamStrategy());

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
        $strategy = $this->createMock(MariaDbPersistenceStrategy::class);
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
}
