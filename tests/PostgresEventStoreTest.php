<?php
/**
 * This file is part of the prooph/pdo-event-store.
 * (c) 2016-2017 prooph software GmbH <contact@prooph.de>
 * (c) 2016-2017 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace ProophTest\EventStore\Pdo;

use PDO;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\Exception\ConcurrencyException;
use Prooph\EventStore\Exception\StreamNotFound;
use Prooph\EventStore\Exception\TransactionAlreadyStarted;
use Prooph\EventStore\Exception\TransactionNotStarted;
use Prooph\EventStore\Metadata\MetadataMatcher;
use Prooph\EventStore\Metadata\Operator;
use Prooph\EventStore\Pdo\Exception\RuntimeException;
use Prooph\EventStore\Pdo\PersistenceStrategy\PostgresAggregateStreamStrategy;
use Prooph\EventStore\Pdo\PersistenceStrategy\PostgresSingleStreamStrategy;
use Prooph\EventStore\Pdo\PostgresEventStore;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;
use ProophTest\EventStore\Mock\TestDomainEvent;
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\Mock\UsernameChanged;
use Ramsey\Uuid\Uuid;

/**
 * @group pdo_pgsql
 */
final class PostgresEventStoreTest extends AbstractPdoEventStoreTest
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
        TestUtil::initDefaultDatabaseTables($this->connection);

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
            new FQCNMessageFactory(),
            new NoOpMessageConverter(),
            $connection,
            new PostgresAggregateStreamStrategy()
        );
    }

    /**
     * @test
     */
    public function it_cannot_create_new_stream_if_table_name_is_already_used(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Error during createSchemaFor');

        $streamName = new StreamName('foo');
        $strategy = new PostgresAggregateStreamStrategy();
        $schema = $strategy->createSchema($strategy->generateTableName($streamName));

        foreach ($schema as $command) {
            $statement = $this->connection->prepare($command);
            $statement->execute();
        }

        $this->eventStore->create(new Stream($streamName, new \ArrayIterator()));
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
    public function it_cannot_commit_twice(): void
    {
        $this->expectException(TransactionNotStarted::class);

        $this->eventStore->beginTransaction();
        $this->eventStore->commit();
        $this->eventStore->commit();
    }

    /**
     * @test
     */
    public function it_can_rollback_empty_transaction(): void
    {
        $this->assertFalse($this->eventStore->inTransaction());
        $this->eventStore->beginTransaction();
        $this->assertTrue($this->eventStore->inTransaction());
        $this->eventStore->rollback();
        $this->assertFalse($this->eventStore->inTransaction());
    }

    /**
     * @test
     */
    public function it_cannot_rollback_twice(): void
    {
        $this->expectException(TransactionNotStarted::class);

        $this->eventStore->beginTransaction();
        $this->eventStore->rollback();
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

    /**
     * @test
     */
    public function it_works_transactional(): void
    {
        $streamName = $this->prophesize(StreamName::class);
        $streamName->toString()->willReturn('Prooph\Model\User')->shouldBeCalled();
        $streamName = $streamName->reveal();

        $stream = $this->prophesize(Stream::class);
        $stream->streamName()->willReturn($streamName);
        $stream->metadata()->willReturn(['foo' => 'bar'])->shouldBeCalled();
        $stream->streamEvents()->willReturn(new \ArrayIterator());

        $this->eventStore->beginTransaction();

        $this->assertFalse($this->eventStore->hasStream($streamName));

        $this->eventStore->create($stream->reveal());

        $this->eventStore->commit();

        $this->assertTrue($this->eventStore->hasStream($streamName));
    }

    /**
     * @test
     */
    public function it_should_rollback_and_throw_exception_in_case_of_transaction_fail()
    {
        $this->expectException(\Exception::class);
        $this->expectExceptionMessage('Transaction failed');

        $eventStore = $this->eventStore;

        $this->eventStore->transactional(function (EventStore $es) use ($eventStore) {
            $this->assertSame($es, $eventStore);
            throw new \Exception('Transaction failed');
        });
    }

    /**
     * @test
     */
    public function it_should_return_true_by_default_if_transaction_is_used()
    {
        $transactionResult = $this->eventStore->transactional(function (EventStore $eventStore) {
            $this->eventStore->create($this->getTestStream());
            $this->assertSame($this->eventStore, $eventStore);
        });
        $this->assertTrue($transactionResult);
    }

    /**
     * @test
     */
    public function it_wraps_up_code_in_transaction_properly()
    {
        $transactionResult = $this->eventStore->transactional(function (EventStore $eventStore) {
            $this->eventStore->create($this->getTestStream());
            $this->assertSame($this->eventStore, $eventStore);

            return 'Result';
        });

        $this->assertSame('Result', $transactionResult);

        $secondStreamEvent = UsernameChanged::with(
            ['new_name' => 'John Doe'],
            2
        );

        $transactionResult = $this->eventStore->transactional(function (EventStore $eventStore) use ($secondStreamEvent) {
            $this->eventStore->appendTo(new StreamName('Prooph\Model\User'), new \ArrayIterator([$secondStreamEvent]));
            $this->assertSame($this->eventStore, $eventStore);

            return 'Second Result';
        });

        $this->assertSame('Second Result', $transactionResult);

        $stream = $this->eventStore->load(new StreamName('Prooph\Model\User'), 1);

        $this->assertCount(2, $stream->streamEvents());
    }
}
