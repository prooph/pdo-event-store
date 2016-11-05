<?php
/**
 * This file is part of the prooph/event-store-pdo-adapter.
 * (c) 2016-2016 prooph software GmbH <contact@prooph.de>
 * (c) 2016-2016 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace ProophTest\EventStore\Adapter\PDO;

use PDO;
use PHPUnit_Framework_TestCase as TestCase;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\Adapter\Exception\RuntimeException;
use Prooph\EventStore\Adapter\PDO\IndexingStrategy\MySQLSingleStreamStrategy;
use Prooph\EventStore\Adapter\PDO\IndexingStrategy\MySQLAggregateStreamStrategy;
use Prooph\EventStore\Adapter\PDO\IndexingStrategy\PostgresAggregateStreamStrategy;
use Prooph\EventStore\Adapter\PDO\IndexingStrategy\PostgresSingleStreamStrategy;
use Prooph\EventStore\Adapter\PDO\JsonQuerier\MySQL;
use Prooph\EventStore\Adapter\PDO\JsonQuerier\Postgres;
use Prooph\EventStore\Adapter\PDO\PDOEventStoreAdapter;
use Prooph\EventStore\Adapter\PDO\TableNameGeneratorStrategy\Sha1;
use Prooph\EventStore\Exception\ConcurrencyException;
use Prooph\EventStore\Metadata\MetadataMatcher;
use Prooph\EventStore\Metadata\Operator;
use Prooph\EventStore\Stream\Stream;
use Prooph\EventStore\Stream\StreamName;
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\Mock\UsernameChanged;
use Ramsey\Uuid\Uuid;

final class PDOEventStoreAdapterTest extends TestCase
{
    /**
     * @var PDOEventStoreAdapter
     */
    private $adapter;

    /**
     * @var PDO
     */
    private $connection;

    protected function setUp(): void
    {
        $this->connection = TestUtil::getConnection();
        switch (TestUtil::getDatabaseVendor()) {
            case 'pdo_mysql':
                $this->connection->exec(file_get_contents(__DIR__ . '/../scripts/mysql_event_streams_table.sql'));
                break;
            case 'pdo_pgsql':
                $this->connection->exec(file_get_contents(__DIR__ . '/../scripts/postgres_event_streams_table.sql'));
                break;
            default:
                throw new \RuntimeException('Unknown database vendor');

        }

        $this->createAdapter($this->connection);
    }

    protected function tearDown(): void
    {
        $this->connection->exec('DROP TABLE event_streams;');
        $this->connection->exec('DROP TABLE _' . sha1('Prooph\Model\User'));
    }

    protected function createAdapter(PDO $connection): void
    {
        switch (TestUtil::getDatabaseVendor()) {
            case 'pdo_mysql':
                $this->adapter = new PDOEventStoreAdapter(
                    new FQCNMessageFactory(),
                    new NoOpMessageConverter(),
                    $connection,
                    new MySQL(),
                    new MySQLAggregateStreamStrategy(),
                    new Sha1()
                );
                break;
            case 'pdo_pgsql':
                $this->adapter = new PDOEventStoreAdapter(
                    new FQCNMessageFactory(),
                    new NoOpMessageConverter(),
                    $connection,
                    new Postgres(),
                    new PostgresAggregateStreamStrategy(),
                    new Sha1()
                );
                break;
            default:
                throw new \RuntimeException('Unknown database vendor');
        }
    }

    /**
     * @test
     */
    public function it_creates_a_stream(): void
    {
        $testStream = $this->getTestStream();

        $this->adapter->beginTransaction();

        $this->adapter->create($testStream);

        $this->adapter->commit();

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher->withMetadataMatch('tag', Operator::EQUALS(), 'person');
        $streamEvents = $this->adapter->loadEvents(new StreamName('Prooph\Model\User'), 0, null, $metadataMatcher);

        $count = 0;
        foreach ($streamEvents as $event) {
            $count++;
        }
        $this->assertEquals(1, $count);

        $testStream->streamEvents()->rewind();
        $streamEvents->rewind();

        $testEvent = $testStream->streamEvents()->current();
        $event = $streamEvents->current();

        $this->assertEquals($testEvent->uuid()->toString(), $event->uuid()->toString());
        $this->assertEquals($testEvent->createdAt()->format('Y-m-d\TH:i:s.uO'), $event->createdAt()->format('Y-m-d\TH:i:s.uO'));
        $this->assertEquals('ProophTest\EventStore\Mock\UserCreated', $event->messageName());
        $this->assertEquals('contact@prooph.de', $event->payload()['email']);
        $this->assertEquals(1, $event->version());
        $this->assertEquals(['tag' => 'person', '_version' => 1], $event->metadata());
    }

    /**
     * @test
     */
    public function it_appends_events_to_a_stream(): void
    {
        $this->adapter->create($this->getTestStream());

        $streamEvent = UsernameChanged::with(
            ['name' => 'John Doe'],
            2
        );

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');

        $this->adapter->appendTo(new StreamName('Prooph\Model\User'), new \ArrayIterator([$streamEvent]));

        $stream = $this->adapter->load(new StreamName('Prooph\Model\User'));

        $this->assertEquals('Prooph\Model\User', $stream->streamName()->toString());

        $count = 0;
        $lastEvent = null;
        foreach ($stream->streamEvents() as $event) {
            $count++;
            $lastEvent = $event;
        }
        $this->assertEquals(2, $count);
        $this->assertInstanceOf(UsernameChanged::class, $lastEvent);
        $messageConverter = new NoOpMessageConverter();

        $streamEventData = $messageConverter->convertToArray($streamEvent);
        $lastEventData = $messageConverter->convertToArray($lastEvent);

        $this->assertEquals($streamEventData, $lastEventData);
    }

    /**
     * @test
     */
    public function it_loads_events_from_position(): void
    {
        $this->adapter->create($this->getTestStream());

        $streamEvent1 = UsernameChanged::with(
            ['name' => 'John Doe'],
            2
        );

        $streamEvent1 = $streamEvent1->withAddedMetadata('tag', 'person');

        $streamEvent2 = UsernameChanged::with(
            ['name' => 'Jane Doe'],
            3
        );

        $streamEvent2 = $streamEvent2->withAddedMetadata('tag', 'person');

        $this->adapter->appendTo(new StreamName('Prooph\Model\User'), new \ArrayIterator([$streamEvent1, $streamEvent2]));

        $stream = $this->adapter->load(new StreamName('Prooph\Model\User'), 2);

        $this->assertEquals('Prooph\Model\User', $stream->streamName()->toString());

        $this->assertTrue($stream->streamEvents()->valid());
        $event = $stream->streamEvents()->current();
        $this->assertEquals(0, $stream->streamEvents()->key());
        $this->assertEquals('John Doe', $event->payload()['name']);

        $stream->streamEvents()->next();
        $this->assertTrue($stream->streamEvents()->valid());
        $event = $stream->streamEvents()->current();
        $this->assertEquals(1, $stream->streamEvents()->key());
        $this->assertEquals('Jane Doe', $event->payload()['name']);

        $stream->streamEvents()->next();
        $this->assertFalse($stream->streamEvents()->valid());
    }

    /**
     * @test
     * @group pdo_mysql
     */
    public function it_fails_to_write_with_duplicate_version_and_one_stream_per_aggregate_strategy_on_pdo_mysql(): void
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

        $this->expectException(ConcurrencyException::class);

        $this->adapter->create($stream);

        $streamEvent = UsernameChanged::with(
            ['name' => 'John Doe'],
            1
        );

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');
        $streamEvent = $streamEvent->withAddedMetadata('_aggregate_id', $aggregateId);
        $streamEvent = $streamEvent->withAddedMetadata('_aggregate_type', 'user');

        $this->adapter->appendTo(new StreamName('Prooph\Model\User'), new \ArrayIterator([$streamEvent]));
    }

    /**
     * @test
     * @group pdo_mysql
     */
    public function it_fails_to_write_with_duplicate_version_and_mulitple_streams_per_aggregate_strategy_on_pdo_mysql(): void
    {
        $this->expectException(ConcurrencyException::class);

        $this->adapter = new PDOEventStoreAdapter(
            new FQCNMessageFactory(),
            new NoOpMessageConverter(),
            $this->connection,
            new MySQL(),
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

        $this->expectException(ConcurrencyException::class);

        $this->adapter->create($stream);

        $streamEvent = UsernameChanged::with(
            ['name' => 'John Doe'],
            1
        );

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');
        $streamEvent = $streamEvent->withAddedMetadata('_aggregate_id', $aggregateId);
        $streamEvent = $streamEvent->withAddedMetadata('_aggregate_type', 'user');

        $this->adapter->appendTo(new StreamName('Prooph\Model\User'), new \ArrayIterator([$streamEvent]));
    }

    /**
     * @test
     * @group pdo_pgsql
     */
    public function it_fails_to_write_with_duplicate_version_and_one_stream_per_aggregate_strategy_on_pdo_pgsql(): void
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

        $this->expectException(ConcurrencyException::class);

        $this->adapter->create($stream);

        $streamEvent = UsernameChanged::with(
            ['name' => 'John Doe'],
            1
        );

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');
        $streamEvent = $streamEvent->withAddedMetadata('_aggregate_id', $aggregateId);
        $streamEvent = $streamEvent->withAddedMetadata('_aggregate_type', 'user');

        $this->adapter->appendTo(new StreamName('Prooph\Model\User'), new \ArrayIterator([$streamEvent]));
    }

    /**
     * @test
     * @group pdo_pgsql
     */
    public function it_fails_to_write_with_duplicate_version_and_mulitple_streams_per_aggregate_strategy_on_pdo_pgsql(): void
    {
        $this->expectException(ConcurrencyException::class);

        $this->adapter = new PDOEventStoreAdapter(
            new FQCNMessageFactory(),
            new NoOpMessageConverter(),
            $this->connection,
            new Postgres(),
            new PostgresSingleStreamStrategy(),
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

        $this->expectException(ConcurrencyException::class);

        $this->adapter->create($stream);

        $streamEvent = UsernameChanged::with(
            ['name' => 'John Doe'],
            1
        );

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');
        $streamEvent = $streamEvent->withAddedMetadata('_aggregate_id', $aggregateId);
        $streamEvent = $streamEvent->withAddedMetadata('_aggregate_type', 'user');

        $this->adapter->appendTo(new StreamName('Prooph\Model\User'), new \ArrayIterator([$streamEvent]));
    }

    /**
     * @test
     */
    public function it_throws_exception_when_empty_stream_created(): void
    {
        $this->expectException(RuntimeException::class);

        $this->adapter->create(new Stream(new StreamName('Prooph\Model\User'), new \ArrayIterator([])));
    }

    /**
     * @test
     */
    public function it_can_rollback_transaction(): void
    {
        $testStream = $this->getTestStream();

        $this->adapter->beginTransaction();

        $this->adapter->create($testStream);

        $this->adapter->rollback();

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch('tag', Operator::EQUALS(), 'person');
        $result = $this->adapter->loadEvents(new StreamName('Prooph\Model\User'), 0, null, $metadataMatcher);

        $this->assertFalse($result->valid());
    }

    /**
     * @test
     */
    public function it_throws_exception_when_second_transaction_started(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Transaction already started');

        $this->adapter->beginTransaction();
        $this->adapter->beginTransaction();
    }

    /**
     * @test
     */
    public function it_can_commit_empty_transaction(): void
    {
        $this->adapter->beginTransaction();
        $this->adapter->commit();
    }

    /**
     * @test
     */
    public function it_can_rollback_empty_transaction(): void
    {
        $this->adapter->beginTransaction();
        $this->adapter->rollback();
    }

    private function getTestStream(): Stream
    {
        $streamEvent = UserCreated::with(
            ['name' => 'Max Mustermann', 'email' => 'contact@prooph.de'],
            1
        );

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');

        return new Stream(new StreamName('Prooph\Model\User'), new \ArrayIterator([$streamEvent]));
    }
}
