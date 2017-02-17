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

use ArrayIterator;
use PDO;
use PHPUnit\Framework\TestCase;
use Prooph\Common\Messaging\Message;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\Exception\StreamExistsAlready;
use Prooph\EventStore\Exception\StreamNotFound;
use Prooph\EventStore\Metadata\MetadataMatcher;
use Prooph\EventStore\Metadata\Operator;
use Prooph\EventStore\Pdo\Exception\InvalidArgumentException;
use Prooph\EventStore\Pdo\Exception\RuntimeException;
use Prooph\EventStore\Projection\ProjectionOptions;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;
use ProophTest\EventStore\Mock\ReadModelMock;
use ProophTest\EventStore\Mock\TestDomainEvent;
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\Mock\UsernameChanged;

abstract class AbstractPdoEventStoreTest extends TestCase
{
    /**
     * @var EventStore
     */
    protected $eventStore;

    /**
     * @var PDO
     */
    protected $connection;

    protected function tearDown(): void
    {
        $this->connection->exec('DROP TABLE event_streams;');
        $this->connection->exec('DROP TABLE _' . sha1('Prooph\Model\User'));
    }

    /**
     * @test
     */
    public function it_appends_events_to_a_stream(): void
    {
        $this->eventStore->create($this->getTestStream());

        $streamEvent = UsernameChanged::with(
            ['name' => 'John Doe'],
            2
        );

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');

        $this->eventStore->appendTo(new StreamName('Prooph\Model\User'), new ArrayIterator([$streamEvent]));

        $streamEvents = $this->eventStore->load(new StreamName('Prooph\Model\User'));

        $count = 0;
        $lastEvent = null;
        foreach ($streamEvents as $event) {
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
    public function it_converts_zero_micros_from_date_time(): void
    {
        $this->eventStore->create($this->getTestStream());

        $streamEvent = UsernameChanged::with(
            ['name' => 'John Doe'],
            2
        );

        $d = $streamEvent->toArray();
        $d['created_at'] = (new \DateTimeImmutable('now', new \DateTimeZone('UTC')))->setTimestamp(time());

        $streamEvent = UsernameChanged::fromArray($d);

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');

        $this->eventStore->appendTo(new StreamName('Prooph\Model\User'), new ArrayIterator([$streamEvent]));

        $streamEvents = $this->eventStore->load(new StreamName('Prooph\Model\User'));

        $this->assertCount(2, $streamEvents);
    }

    /**
     * @test
     */
    public function it_loads_events_from_position(): void
    {
        $this->eventStore->create($this->getTestStream());

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

        $this->eventStore->appendTo(new StreamName('Prooph\Model\User'), new \ArrayIterator([$streamEvent1, $streamEvent2]));

        $streamEvents = $this->eventStore->load(new StreamName('Prooph\Model\User'), 2);

        $this->assertTrue($streamEvents->valid());
        $event = $streamEvents->current();
        $this->assertEquals(0, $streamEvents->key());
        $this->assertEquals('John Doe', $event->payload()['name']);

        $streamEvents->next();
        $this->assertTrue($streamEvents->valid());
        $event = $streamEvents->current();
        $this->assertEquals(1, $streamEvents->key());
        $this->assertEquals('Jane Doe', $event->payload()['name']);

        $streamEvents->next();
        $this->assertFalse($streamEvents->valid());
    }

    /**
     * @test
     */
    public function it_appends_events_to_stream_and_records_them(): void
    {
        $this->eventStore->create($this->getTestStream());

        $secondStreamEvent = UsernameChanged::with(
            ['new_name' => 'John Doe'],
            2
        );

        $this->eventStore->appendTo(new StreamName('Prooph\Model\User'), new ArrayIterator([$secondStreamEvent]));

        $this->assertCount(2, $this->eventStore->load(new StreamName('Prooph\Model\User')));
    }

    /**
     * @test
     * @dataProvider getMatchingMetadata
     */
    public function it_loads_events_by_matching_metadata(array $metadata): void
    {
        $stream = $this->getTestStream();

        $this->eventStore->create($stream);

        $streamEventWithMetadata = TestDomainEvent::with(
            ['name' => 'Alex', 'email' => 'contact@prooph.de'],
            2
        );

        foreach ($metadata as $field => $value) {
            $streamEventWithMetadata = $streamEventWithMetadata->withAddedMetadata($field, $value);
        }

        $this->eventStore->appendTo($stream->streamName(), new ArrayIterator([$streamEventWithMetadata]));

        $metadataMatcher = new MetadataMatcher();

        foreach ($metadata as $field => $value) {
            $metadataMatcher = $metadataMatcher->withMetadataMatch($field, Operator::EQUALS(), $value);
        }

        $streamEvents = $this->eventStore->load($stream->streamName(), 1, null, $metadataMatcher);

        $this->assertCount(1, $streamEvents);

        $streamEvents->rewind();

        $currentMetadata = $streamEvents->current()->metadata();

        foreach ($metadata as $field => $value) {
            $this->assertEquals($value, $currentMetadata[$field]);
        }
    }

    /**
     * @test
     * @dataProvider getMatchingMetadata
     */
    public function it_loads_events_reverse_by_matching_metadata(array $metadata): void
    {
        $stream = $this->getTestStream();

        $this->eventStore->create($stream);

        $streamEventWithMetadata = TestDomainEvent::with(
            ['name' => 'Alex', 'email' => 'contact@prooph.de'],
            2
        );

        foreach ($metadata as $field => $value) {
            $streamEventWithMetadata = $streamEventWithMetadata->withAddedMetadata($field, $value);
        }

        $this->eventStore->appendTo($stream->streamName(), new ArrayIterator([$streamEventWithMetadata]));

        $metadataMatcher = new MetadataMatcher();

        foreach ($metadata as $field => $value) {
            $metadataMatcher = $metadataMatcher->withMetadataMatch($field, Operator::EQUALS(), $value);
        }

        $streamEvents = $this->eventStore->loadReverse($stream->streamName(), 2, null, $metadataMatcher);

        $this->assertCount(1, $streamEvents);

        $streamEvents->rewind();

        $currentMetadata = $streamEvents->current()->metadata();

        foreach ($metadata as $field => $value) {
            $this->assertEquals($value, $currentMetadata[$field]);
        }
    }

    /**
     * @test
     */
    public function it_loads_events_from_number(): void
    {
        $stream = $this->getTestStream();

        $this->eventStore->create($stream);

        $streamEventVersion2 = UsernameChanged::with(
            ['new_name' => 'John Doe'],
            2
        );

        $streamEventVersion2 = $streamEventVersion2->withAddedMetadata('snapshot', true);

        $streamEventVersion3 = UsernameChanged::with(
            ['new_name' => 'Jane Doe'],
            3
        );

        $streamEventVersion3 = $streamEventVersion3->withAddedMetadata('snapshot', false);

        $this->eventStore->appendTo($stream->streamName(), new ArrayIterator([$streamEventVersion2, $streamEventVersion3]));

        $loadedEvents = $this->eventStore->load($stream->streamName(), 2);

        $this->assertCount(2, $loadedEvents);

        $loadedEvents->rewind();

        $this->assertTrue($loadedEvents->current()->metadata()['snapshot']);
        $loadedEvents->next();
        $this->assertFalse($loadedEvents->current()->metadata()['snapshot']);

        $streamEvents = $this->eventStore->load($stream->streamName(), 2);

        $this->assertCount(2, $streamEvents);

        $streamEvents->rewind();

        $this->assertTrue($streamEvents->current()->metadata()['snapshot']);
        $streamEvents->next();
        $this->assertFalse($streamEvents->current()->metadata()['snapshot']);
    }

    /**
     * @test
     */
    public function it_loads_events_reverse_from_number(): void
    {
        $stream = $this->getTestStream();

        $this->eventStore->create($stream);

        $streamEventVersion2 = UsernameChanged::with(
            ['new_name' => 'John Doe'],
            2
        );

        $streamEventVersion2 = $streamEventVersion2->withAddedMetadata('snapshot', true);

        $streamEventVersion3 = UsernameChanged::with(
            ['new_name' => 'Jane Doe'],
            3
        );

        $streamEventVersion3 = $streamEventVersion3->withAddedMetadata('snapshot', false);

        $this->eventStore->appendTo($stream->streamName(), new ArrayIterator([$streamEventVersion2, $streamEventVersion3]));

        $loadedEvents = $this->eventStore->loadReverse($stream->streamName(), PHP_INT_MAX, 2);

        $this->assertCount(2, $loadedEvents);

        $loadedEvents->rewind();

        $this->assertFalse($loadedEvents->current()->metadata()['snapshot']);
        $loadedEvents->next();
        $this->assertTrue($loadedEvents->current()->metadata()['snapshot']);

        $streamEvents = $this->eventStore->loadReverse($stream->streamName(), PHP_INT_MAX, 2);

        $this->assertCount(2, $streamEvents);

        $streamEvents->rewind();

        $this->assertFalse($streamEvents->current()->metadata()['snapshot']);
        $streamEvents->next();
        $this->assertTrue($streamEvents->current()->metadata()['snapshot']);
    }

    /**
     * @test
     */
    public function it_loads_events_from_number_with_count(): void
    {
        $stream = $this->getTestStream();

        $this->eventStore->create($stream);

        $streamEventVersion2 = UsernameChanged::with(
            ['new_name' => 'John Doe'],
            2
        );

        $streamEventVersion2 = $streamEventVersion2->withAddedMetadata('snapshot', true);

        $streamEventVersion3 = UsernameChanged::with(
            ['new_name' => 'Jane Doe'],
            3
        );

        $streamEventVersion3 = $streamEventVersion3->withAddedMetadata('snapshot', false);

        $streamEventVersion4 = UsernameChanged::with(
            ['new_name' => 'Jane Dole'],
            4
        );

        $streamEventVersion4 = $streamEventVersion4->withAddedMetadata('snapshot', false);

        $this->eventStore->appendTo($stream->streamName(), new ArrayIterator([
            $streamEventVersion2,
            $streamEventVersion3,
            $streamEventVersion4,
        ]));

        $loadedEvents = $this->eventStore->load($stream->streamName(), 2, 2);

        $this->assertCount(2, $loadedEvents);

        $loadedEvents->rewind();

        $this->assertTrue($loadedEvents->current()->metadata()['snapshot']);
        $loadedEvents->next();
        $this->assertFalse($loadedEvents->current()->metadata()['snapshot']);

        $loadedEvents = $this->eventStore->load($stream->streamName(), 2, 2);

        $this->assertCount(2, $loadedEvents);

        $loadedEvents->rewind();

        $this->assertTrue($loadedEvents->current()->metadata()['snapshot']);
        $loadedEvents->next();
        $this->assertFalse($loadedEvents->current()->metadata()['snapshot']);
    }

    /**
     * @test
     */
    public function it_loads_events_reverse_from_number_with_count(): void
    {
        $stream = $this->getTestStream();

        $this->eventStore->create($stream);

        $streamEventVersion2 = UsernameChanged::with(
            ['new_name' => 'John Doe'],
            2
        );

        $streamEventVersion2 = $streamEventVersion2->withAddedMetadata('snapshot', true);

        $streamEventVersion3 = UsernameChanged::with(
            ['new_name' => 'Jane Doe'],
            3
        );

        $streamEventVersion3 = $streamEventVersion3->withAddedMetadata('snapshot', false);

        $streamEventVersion4 = UsernameChanged::with(
            ['new_name' => 'Jane Dole'],
            4
        );

        $streamEventVersion4 = $streamEventVersion4->withAddedMetadata('snapshot', false);

        $this->eventStore->appendTo($stream->streamName(), new ArrayIterator([
            $streamEventVersion2,
            $streamEventVersion3,
            $streamEventVersion4,
        ]));

        $loadedEvents = $this->eventStore->loadReverse($stream->streamName(), 3, 2);

        $this->assertCount(2, $loadedEvents);

        $loadedEvents->rewind();

        $this->assertFalse($loadedEvents->current()->metadata()['snapshot']);
        $loadedEvents->next();
        $this->assertTrue($loadedEvents->current()->metadata()['snapshot']);

        $loadedEvents = $this->eventStore->loadReverse($stream->streamName(), 3, 2);

        $this->assertCount(2, $loadedEvents);

        $loadedEvents->rewind();

        $this->assertFalse($loadedEvents->current()->metadata()['snapshot']);
        $loadedEvents->next();
        $this->assertTrue($loadedEvents->current()->metadata()['snapshot']);
    }

    /**
     * @test
     */
    public function it_loads_events_in_reverse_order(): void
    {
        $stream = $this->getTestStream();

        $this->eventStore->create($stream);

        $streamEventVersion2 = UsernameChanged::with(
            ['new_name' => 'John Doe'],
            2
        );

        $streamEventVersion2 = $streamEventVersion2->withAddedMetadata('snapshot', true);

        $streamEventVersion3 = UsernameChanged::with(
            ['new_name' => 'Jane Doe'],
            3
        );

        $streamEventVersion3 = $streamEventVersion3->withAddedMetadata('snapshot', false);

        $streamEventVersion4 = UsernameChanged::with(
            ['new_name' => 'Jane Dole'],
            4
        );

        $streamEventVersion4 = $streamEventVersion4->withAddedMetadata('snapshot', false);

        $this->eventStore->appendTo($stream->streamName(), new ArrayIterator([
            $streamEventVersion2,
            $streamEventVersion3,
            $streamEventVersion4,
        ]));

        $loadedEvents = $this->eventStore->loadReverse($stream->streamName(), 3, 2);

        $this->assertCount(2, $loadedEvents);

        $loadedEvents->rewind();

        $this->assertFalse($loadedEvents->current()->metadata()['snapshot']);
        $loadedEvents->next();
        $this->assertTrue($loadedEvents->current()->metadata()['snapshot']);
    }

    /**
     * @test
     */
    public function it_throws_stream_not_found_exception_if_it_loads_nothing(): void
    {
        $this->expectException(StreamNotFound::class);

        $stream = $this->getTestStream();

        $this->eventStore->load($stream->streamName());
    }

    /**
     * @test
     */
    public function it_throws_stream_not_found_exception_if_it_loads_nothing_reverse(): void
    {
        $this->expectException(StreamNotFound::class);

        $stream = $this->getTestStream();

        $this->eventStore->loadReverse($stream->streamName());
    }

    /**
     * @test
     */
    public function it_throws_exception_when_asked_for_unknown_stream_metadata(): void
    {
        $this->expectException(StreamNotFound::class);

        $this->eventStore->fetchStreamMetadata(new StreamName('unknown'));
    }

    /**
     * @test
     */
    public function it_returns_metadata_when_asked_for_stream_metadata(): void
    {
        $stream = new Stream(new StreamName('Prooph\Model\User'), new ArrayIterator(), ['foo' => 'bar']);

        $this->eventStore->create($stream);

        $this->assertEquals(['foo' => 'bar'], $this->eventStore->fetchStreamMetadata($stream->streamName()));
    }

    /**
     * @test
     */
    public function it_returns_only_matched_metadata(): void
    {
        $event = UserCreated::with(['name' => 'John'], 1);
        $event = $event->withAddedMetadata('foo', 'bar');
        $event = $event->withAddedMetadata('int', 5);
        $event = $event->withAddedMetadata('int2', 4);
        $event = $event->withAddedMetadata('int3', 6);
        $event = $event->withAddedMetadata('int4', 7);

        $stream = new Stream(new StreamName('Prooph\Model\User'), new ArrayIterator([$event]));

        $this->eventStore->create($stream);

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch('foo', Operator::EQUALS(), 'bar');
        $metadataMatcher = $metadataMatcher->withMetadataMatch('foo', Operator::NOT_EQUALS(), 'baz');
        $metadataMatcher = $metadataMatcher->withMetadataMatch('int', Operator::GREATER_THAN(), 4);
        $metadataMatcher = $metadataMatcher->withMetadataMatch('int2', Operator::GREATER_THAN_EQUALS(), 4);
        $metadataMatcher = $metadataMatcher->withMetadataMatch('int3', Operator::LOWER_THAN(), 7);
        $metadataMatcher = $metadataMatcher->withMetadataMatch('int4', Operator::LOWER_THAN_EQUALS(), 7);

        $streamEvents = $this->eventStore->load($stream->streamName(), 1, null, $metadataMatcher);

        $this->assertCount(1, $streamEvents);
    }

    /**
     * @test
     */
    public function it_returns_only_matched_metadata_reverse(): void
    {
        $event = UserCreated::with(['name' => 'John'], 1);
        $event = $event->withAddedMetadata('foo', 'bar');
        $event = $event->withAddedMetadata('int', 5);
        $event = $event->withAddedMetadata('int2', 4);
        $event = $event->withAddedMetadata('int3', 6);
        $event = $event->withAddedMetadata('int4', 7);

        $stream = new Stream(new StreamName('Prooph\Model\User'), new ArrayIterator([$event]));

        $this->eventStore->create($stream);

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch('foo', Operator::EQUALS(), 'bar');
        $metadataMatcher = $metadataMatcher->withMetadataMatch('foo', Operator::NOT_EQUALS(), 'baz');
        $metadataMatcher = $metadataMatcher->withMetadataMatch('int', Operator::GREATER_THAN(), 4);
        $metadataMatcher = $metadataMatcher->withMetadataMatch('int2', Operator::GREATER_THAN_EQUALS(), 4);
        $metadataMatcher = $metadataMatcher->withMetadataMatch('int3', Operator::LOWER_THAN(), 7);
        $metadataMatcher = $metadataMatcher->withMetadataMatch('int4', Operator::LOWER_THAN_EQUALS(), 7);

        $streamEvents = $this->eventStore->loadReverse($stream->streamName(), 1, null, $metadataMatcher);

        $this->assertCount(1, $streamEvents);
    }

    /**
     * @test
     */
    public function it_deletes_stream(): void
    {
        $stream = $this->getTestStream();

        $this->eventStore->create($stream);

        $this->eventStore->delete($stream->streamName());

        $this->assertFalse($this->eventStore->hasStream($stream->streamName()));
    }

    /**
     * @test
     */
    public function it_loads_empty_stream(): void
    {
        $streamName = new StreamName('Prooph\Model\User');

        $this->eventStore->create(new Stream($streamName, new ArrayIterator()));

        $it = $this->eventStore->load($streamName);

        $this->assertInstanceOf(\EmptyIterator::class, $it);
    }

    /**
     * @test
     */
    public function it_loads_reverse_empty_stream(): void
    {
        $streamName = new StreamName('Prooph\Model\User');

        $this->eventStore->create(new Stream($streamName, new ArrayIterator()));

        $it = $this->eventStore->loadReverse($streamName);

        $this->assertInstanceOf(\EmptyIterator::class, $it);
    }

    /**
     * @test
     */
    public function it_throws_exception_when_trying_to_delete_unknown_stream(): void
    {
        $this->expectException(StreamNotFound::class);

        $this->eventStore->delete(new StreamName('unknown'));
    }

    /**
     * @test
     */
    public function it_can_check_for_stream_existence(): void
    {
        $streamName = new StreamName('Prooph\Model\User');

        $this->assertFalse($this->eventStore->hasStream($streamName));

        $this->eventStore->create($this->getTestStream());

        $this->assertTrue($this->eventStore->hasStream($streamName));
    }

    /**
     * @test
     */
    public function it_throws_exception_when_trying_to_append_on_non_existing_stream(): void
    {
        $this->expectException(StreamNotFound::class);

        $event = UserCreated::with(['name' => 'Alex'], 1);

        $this->eventStore->appendTo(new StreamName('unknown'), new ArrayIterator([$event]));
    }

    /**
     * @test
     */
    public function it_appends_an_empty_stream(): void
    {
        $this->eventStore->appendTo(new StreamName('something'), new ArrayIterator());

        $this->assertFalse($this->eventStore->hasStream(new StreamName('something')));
    }

    /**
     * @test
     */
    public function it_throws_exception_when_trying_to_load_non_existing_stream(): void
    {
        $this->expectException(StreamNotFound::class);

        $streamName = $this->prophesize(StreamName::class);
        $streamName->toString()->willReturn('test');

        $this->eventStore->load($streamName->reveal());
    }

    /**
     * @test
     */
    public function it_throws_stream_not_found_exception_when_trying_to_update_metadata_on_unknown_stream(): void
    {
        $this->expectException(StreamNotFound::class);

        $this->eventStore->updateStreamMetadata(new StreamName('unknown'), []);
    }

    /**
     * @test
     */
    public function it_updates_stream_metadata(): void
    {
        $stream = $this->getTestStream();

        $this->eventStore->create($stream);

        $this->eventStore->updateStreamMetadata($stream->streamName(), ['new' => 'values']);

        $this->assertEquals(
            [
                'new' => 'values',
            ],
            $this->eventStore->fetchStreamMetadata($stream->streamName())
        );
    }

    /**
     * @test
     */
    public function it_cannot_create_stream_twice(): void
    {
        $this->expectException(StreamExistsAlready::class);

        $stream = $this->getTestStream();

        $this->eventStore->create($stream);
        $this->eventStore->create($stream);
    }

    /**
     * @test
     */
    public function it_throws_exception_when_base_projection_options_given_to_projection(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $this->eventStore->createProjection('foo', new ProjectionOptions());
    }

    /**
     * @test
     */
    public function it_throws_exception_when_base_projection_options_given_to_read_model_projection(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $this->eventStore->createReadModelProjection('foo', new ReadModelMock(), new ProjectionOptions());
    }

    /**
     * @test
     */
    public function it_handles_not_existing_event_streams_table(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Maybe the event streams table is not setup?');

        $this->connection->exec('DROP TABLE event_streams;');

        $this->eventStore->create($this->getTestStream());
    }

    public function getMatchingMetadata(): array
    {
        return [
            [['snapshot' => true]],
            [['some_id' => 123]],
            [['fuu' => 'bar']],
            [['snapshot' => true, 'some_id' => 123, 'fuu' => 'bar']],
        ];
    }

    protected function getTestStream(): Stream
    {
        $streamEvent = UserCreated::with(
            ['name' => 'Max Mustermann', 'email' => 'contact@prooph.de'],
            1
        );

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');

        return new Stream(new StreamName('Prooph\Model\User'), new ArrayIterator([$streamEvent]));
    }

    /**
     * @return Message[]
     */
    protected function getMultipleTestEvents(): array
    {
        $events = [];

        $event = UserCreated::with(['name' => 'Alex'], 1);
        $events[] = $event->withAddedMetadata('_aggregate_id', 'one')->withAddedMetadata('_aggregate_type', 'user');

        $event = UserCreated::with(['name' => 'Sascha'], 1);
        $events[] = $event->withAddedMetadata('_aggregate_id', 'two')->withAddedMetadata('_aggregate_type', 'user');

        for ($i = 2; $i < 100; $i++) {
            $event = UsernameChanged::with(['name' => uniqid('name_')], $i);
            $events[] = $event->withAddedMetadata('_aggregate_id', 'two')->withAddedMetadata('_aggregate_type', 'user');

            $event = UsernameChanged::with(['name' => uniqid('name_')], $i);
            $events[] = $event->withAddedMetadata('_aggregate_id', 'one')->withAddedMetadata('_aggregate_type', 'user');
        }

        $event = UsernameChanged::with(['name' => 'Sandro'], 100);
        $events[] = $event->withAddedMetadata('_aggregate_id', 'one')->withAddedMetadata('_aggregate_type', 'user');

        $event = UsernameChanged::with(['name' => 'Bradley'], 100);
        $events[] = $event->withAddedMetadata('_aggregate_id', 'two')->withAddedMetadata('_aggregate_type', 'user');

        return $events;
    }

    /**
     * @test
     */
    public function it_fetches_stream_names(): void
    {
        $streamNames = [];

        try {
            for ($i = 0; $i < 50; $i++) {
                $streamNames[] = 'user-' . $i;
                $streamNames[] = 'admin-' . $i;
                $this->eventStore->create(new Stream(new StreamName('user-' . $i), new \EmptyIterator(), ['foo' => 'bar']));
                $this->eventStore->create(new Stream(new StreamName('admin-' . $i), new \EmptyIterator(), ['foo' => 'bar']));
            }

            for ($i = 0; $i < 20; $i++) {
                $streamName = uniqid('rand');
                $streamNames[] = $streamName;
                $this->eventStore->create(new Stream(new StreamName($streamName), new \EmptyIterator()));
            }

            $this->assertCount(120, $this->eventStore->fetchStreamNames(null, false, null, 200, 0));
            $this->assertCount(0, $this->eventStore->fetchStreamNames(null, false, null, 200, 200));
            $this->assertCount(10, $this->eventStore->fetchStreamNames(null, false, null, 10, 0));
            $this->assertCount(10, $this->eventStore->fetchStreamNames(null, false, null, 10, 10));
            $this->assertCount(5, $this->eventStore->fetchStreamNames(null, false, null, 10, 115));

            for ($i = 0; $i < 50; $i++) {
                $this->assertStringStartsWith('admin-', $this->eventStore->fetchStreamNames(null, false, null, 1, $i)[0]->toString());
            }

            for ($i = 50; $i < 70; $i++) {
                $this->assertStringStartsWith('rand', $this->eventStore->fetchStreamNames(null, false, null, 1, $i)[0]->toString());
            }

            for ($i = 0; $i < 50; $i++) {
                $this->assertStringStartsWith('user-', $this->eventStore->fetchStreamNames(null, false, null, 1, $i + 70)[0]->toString());
            }

            $this->assertCount(30, $this->eventStore->fetchStreamNames('s.*er-', true, null, 30, 0));
            $this->assertCount(30, $this->eventStore->fetchStreamNames('n.*-', true, (new MetadataMatcher())->withMetadataMatch('foo', Operator::EQUALS(), 'bar'), 30, 0));
            $this->assertCount(0, $this->eventStore->fetchStreamNames('n.*-', true, (new MetadataMatcher())->withMetadataMatch('foo', Operator::NOT_EQUALS(), 'bar'), 30, 0));
            $this->assertCount(0, $this->eventStore->fetchStreamNames(null, false, (new MetadataMatcher())->withMetadataMatch('foo', Operator::NOT_EQUALS(), 'bar'), 30, 0));
        } finally {
            foreach ($streamNames as $streamName) {
                $this->eventStore->delete(new StreamName($streamName));
            }
        }
    }

    /**
     * @test
     */
    public function it_throws_exception_when_fetching_stream_names_using_regex_and_no_filter(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('No regex pattern given');

        $this->eventStore->fetchStreamNames(null, true, null, 10, 0);
    }

    /**
     * @test
     */
    public function it_throws_exception_when_fetching_stream_names_using_invalid_regex(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid regex pattern given');

        $this->eventStore->fetchStreamNames('invalid)', true, null, 10, 0);
    }

    /**
     * @test
     * @group by
     */
    public function it_fetches_stream_categories(): void
    {
        try {
            for ($i = 0; $i < 5; $i++) {
                $this->eventStore->create(new Stream(new StreamName('foo-' . $i), new \EmptyIterator()));
                $this->eventStore->create(new Stream(new StreamName('bar-' . $i), new \EmptyIterator()));
                $this->eventStore->create(new Stream(new StreamName('baz-' . $i), new \EmptyIterator()));
                $this->eventStore->create(new Stream(new StreamName('bam-' . $i), new \EmptyIterator()));
                $this->eventStore->create(new Stream(new StreamName('foobar-' . $i), new \EmptyIterator()));
                $this->eventStore->create(new Stream(new StreamName('foobaz-' . $i), new \EmptyIterator()));
                $this->eventStore->create(new Stream(new StreamName('foobam-' . $i), new \EmptyIterator()));
            }

            for ($i = 0; $i < 20; $i++) {
                $this->eventStore->create(new Stream(new StreamName(uniqid('rand')), new \EmptyIterator()));
            }

            $this->assertCount(7, $this->eventStore->fetchCategoryNames(null, false, 20, 0));
            $this->assertCount(0, $this->eventStore->fetchCategoryNames(null, false, 20, 20));
            $this->assertCount(3, $this->eventStore->fetchCategoryNames(null, false, 3, 0));
            $this->assertCount(3, $this->eventStore->fetchCategoryNames(null, false, 3, 3));
            $this->assertCount(5, $this->eventStore->fetchCategoryNames(null, false, 10, 2));

            $this->assertCount(1, $this->eventStore->fetchCategoryNames('foo', false, 20, 0));
            $this->assertCount(4, $this->eventStore->fetchCategoryNames('foo', true, 20, 0));
        } finally {
            $databaseName = TestUtil::getDatabaseName();
            $dropTables = <<<EOT
SET FOREIGN_KEY_CHECKS = 0;
SET GROUP_CONCAT_MAX_LEN=32768;
SET @tables = NULL;
SELECT GROUP_CONCAT('`', table_name, '`') INTO @tables
  FROM information_schema.tables
  WHERE table_schema = '$databaseName';
SELECT IFNULL(@tables,'dummy') INTO @tables;

SET @tables = CONCAT('DROP TABLE IF EXISTS ', @tables);
PREPARE stmt FROM @tables;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
SET FOREIGN_KEY_CHECKS = 1;
EOT;

            $this->connection->query($dropTables);
            TestUtil::initDefaultDatabaseTables($this->connection);
        }
    }

    /**
     * @test
     */
    public function it_throws_exception_when_fetching_stream_categories_using_regex_and_no_filter(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('No regex pattern given');

        $this->eventStore->fetchCategoryNames(null, true, 10, 0);
    }

    /**
     * @test
     */
    public function it_throws_exception_when_fetching_stream_categories_using_invalid_regex(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid regex pattern given');

        $this->eventStore->fetchCategoryNames('invalid)', true, 10, 0);
    }
}
