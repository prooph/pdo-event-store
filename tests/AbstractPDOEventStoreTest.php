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

use ArrayIterator;
use PDO;
use PHPUnit_Framework_TestCase as TestCase;
use Prooph\Common\Event\ActionEvent;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\ActionEventEmitterAware;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\Exception\StreamExistsAlready;
use Prooph\EventStore\Exception\StreamNotFound;
use Prooph\EventStore\Metadata\MetadataMatcher;
use Prooph\EventStore\Metadata\Operator;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;
use ProophTest\EventStore\Mock\TestDomainEvent;
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\Mock\UsernameChanged;

abstract class AbstractPDOEventStoreTest extends TestCase
{
    /**
     * @var EventStore|ActionEventEmitterAware
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

        $stream = $this->eventStore->load(new StreamName('Prooph\Model\User'));

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

        $stream = $this->eventStore->load(new StreamName('Prooph\Model\User'), 2);

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
     */
    public function it_throws_exception_when_listener_stops_propagation(): void
    {
        $this->expectException(StreamExistsAlready::class);

        $this->eventStore->getActionEventEmitter()->attachListener(
            'create',
            function (ActionEvent $event): void {
                $event->stopPropagation(true);
            },
            1000
        );

        $stream = $this->getTestStream();

        $this->eventStore->create($stream);
    }

    /**
     * @test
     */
    public function it_appends_events_to_stream_and_records_them(): void
    {
        $recordedEvents = [];

        $this->eventStore->getActionEventEmitter()->attachListener(
            'appendTo',
            function (ActionEvent $event) use (&$recordedEvents): void {
                foreach ($event->getParam('streamEvents', new \ArrayIterator()) as $recordedEvent) {
                    $recordedEvents[] = $recordedEvent;
                }
            },
            -1000
        );

        $this->eventStore->create($this->getTestStream());

        $secondStreamEvent = UsernameChanged::with(
            ['new_name' => 'John Doe'],
            2
        );

        $this->eventStore->appendTo(new StreamName('Prooph\Model\User'), new ArrayIterator([$secondStreamEvent]));

        $this->assertCount(2, $recordedEvents);
    }

    /**
     * @test
     */
    public function it_does_not_append_events_when_listener_stops_propagation(): void
    {
        $this->expectException(StreamNotFound::class);

        $recordedEvents = [];

        $this->eventStore->getActionEventEmitter()->attachListener(
            'create',
            function (ActionEvent $event) use (&$recordedEvents): void {
                foreach ($event->getParam('recordedEvents', new \ArrayIterator()) as $recordedEvent) {
                    $recordedEvents[] = $recordedEvent;
                }
            },
            -1000
        );

        $this->eventStore->getActionEventEmitter()->attachListener(
            'appendTo',
            function (ActionEvent $event) use (&$recordedEvents): void {
                foreach ($event->getParam('recordedEvents', new \ArrayIterator()) as $recordedEvent) {
                    $recordedEvents[] = $recordedEvent;
                }
            },
            -1000
        );

        $this->eventStore->create($this->getTestStream());

        $this->eventStore->getActionEventEmitter()->attachListener(
            'appendTo',
            function (ActionEvent $event): void {
                $event->setParam('stream', false);
                $event->stopPropagation(true);
            },
            1000
        );

        $secondStreamEvent = UsernameChanged::with(
            ['new_name' => 'John Doe'],
            2
        );

        $this->eventStore->appendTo(new StreamName('user'), new ArrayIterator([$secondStreamEvent]));
    }

    /**
     * @test
     */
    public function it_loads_events_by_matching_metadata(): void
    {
        $stream = $this->getTestStream();

        $this->eventStore->create($stream);

        $streamEventWithMetadata = TestDomainEvent::with(
            ['name' => 'Alex', 'email' => 'contact@prooph.de'],
            2
        );

        $streamEventWithMetadata = $streamEventWithMetadata->withAddedMetadata('snapshot', true);

        $this->eventStore->appendTo($stream->streamName(), new ArrayIterator([$streamEventWithMetadata]));

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch('snapshot', Operator::EQUALS(), true);

        $stream = $this->eventStore->load($stream->streamName(), 1, null, $metadataMatcher);

        $this->assertCount(1, $stream->streamEvents());

        $stream->streamEvents()->rewind();

        $this->assertTrue($stream->streamEvents()->current()->metadata()['snapshot']);
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

        $stream = $this->eventStore->load($stream->streamName(), 2);
        $loadedEvents = $stream->streamEvents();

        $this->assertCount(2, $loadedEvents);

        $loadedEvents->rewind();

        $this->assertTrue($loadedEvents->current()->metadata()['snapshot']);
        $loadedEvents->next();
        $this->assertFalse($loadedEvents->current()->metadata()['snapshot']);

        $stream = $this->eventStore->load($stream->streamName(), 2);

        $this->assertCount(2, $stream->streamEvents());

        $stream->streamEvents()->rewind();

        $this->assertTrue($stream->streamEvents()->current()->metadata()['snapshot']);
        $stream->streamEvents()->next();
        $this->assertFalse($stream->streamEvents()->current()->metadata()['snapshot']);
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
            $streamEventVersion4
        ]));

        $stream = $this->eventStore->load($stream->streamName(), 2, 2);
        $loadedEvents = $stream->streamEvents();

        $this->assertCount(2, $loadedEvents);

        $loadedEvents->rewind();

        $this->assertTrue($loadedEvents->current()->metadata()['snapshot']);
        $loadedEvents->next();
        $this->assertFalse($loadedEvents->current()->metadata()['snapshot']);

        $stream = $this->eventStore->load($stream->streamName(), 2, 2);

        $loadedEvents = $stream->streamEvents();

        $this->assertCount(2, $loadedEvents);

        $loadedEvents->rewind();

        $this->assertTrue($loadedEvents->current()->metadata()['snapshot']);
        $loadedEvents->next();
        $this->assertFalse($loadedEvents->current()->metadata()['snapshot']);
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
            $streamEventVersion4
        ]));

        $stream = $this->eventStore->loadReverse($stream->streamName(), 3, 2);
        $loadedEvents = $stream->streamEvents();

        $this->assertCount(2, $loadedEvents);

        $loadedEvents->rewind();

        $this->assertFalse($loadedEvents->current()->metadata()['snapshot']);
        $loadedEvents->next();
        $this->assertTrue($loadedEvents->current()->metadata()['snapshot']);
    }

    /**
     * @test
     */
    public function it_throws_exception_when_listener_stops_loading_events_and_does_not_provide_loaded_events(): void
    {
        $this->expectException(StreamNotFound::class);

        $stream = $this->getTestStream();

        $this->eventStore->create($stream);

        $streamEventWithMetadata = UsernameChanged::with(
            ['new_name' => 'John Doe'],
            2
        );

        $streamEventWithMetadata = $streamEventWithMetadata->withAddedMetadata('snapshot', true);

        $this->eventStore->appendTo($stream->streamName(), new ArrayIterator([$streamEventWithMetadata]));

        $this->eventStore->getActionEventEmitter()->attachListener(
            'load',
            function (ActionEvent $event): void {
                $event->stopPropagation(true);
            },
            1000
        );

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch('snapshot', Operator::EQUALS(), true);

        $this->eventStore->load($stream->streamName(), 1, null, $metadataMatcher);
    }

    /**
     * @test
     */
    public function it_returns_listener_events_when_listener_stops_loading_events_and_provide_loaded_events(): void
    {
        $stream = $this->getTestStream();

        $this->eventStore->create($stream);

        $streamEventWithMetadata = UsernameChanged::with(
            ['new_name' => 'John Doe'],
            2
        );

        $streamEventWithMetadata = $streamEventWithMetadata->withAddedMetadata('snapshot', true);

        $this->eventStore->appendTo($stream->streamName(), new ArrayIterator([$streamEventWithMetadata]));

        $this->eventStore->getActionEventEmitter()->attachListener(
            'load',
            function (ActionEvent $event): void {
                $streamEventWithMetadataButOtherUuid = UsernameChanged::with(
                    ['new_name' => 'John Doe'],
                    2
                );

                $streamEventWithMetadataButOtherUuid = $streamEventWithMetadataButOtherUuid->withAddedMetadata('snapshot', true);

                $streamName = $event->getParam('streamName');

                $event->setParam('stream', new Stream(
                        $streamName,
                        new ArrayIterator([$streamEventWithMetadataButOtherUuid]))
                );
                $event->stopPropagation(true);
            },
            1000
        );

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch('snapshot', Operator::EQUALS(), true);

        $stream = $this->eventStore->load($stream->streamName(), 1, null, $metadataMatcher);
        $loadedEvents = $stream->streamEvents();

        $this->assertCount(1, $loadedEvents);

        $loadedEvents->rewind();

        $this->assertNotEquals($streamEventWithMetadata->uuid()->toString(), $loadedEvents->current()->uuid()->toString());
    }

    /**
     * @test
     */
    public function it_breaks_loading_a_stream_when_listener_stops_propagation_but_does_not_provide_a_stream(): void
    {
        $this->expectException(StreamNotFound::class);
        $stream = $this->getTestStream();

        $this->eventStore->create($stream);

        $this->eventStore->getActionEventEmitter()->attachListener(
            'load',
            function (ActionEvent $event): void {
                $event->stopPropagation(true);
            },
            1000
        );

        $this->eventStore->load(new StreamName('user'));
    }

    /**
     * @test
     */
    public function it_breaks_loading_a_stream_when_listener_stops_propagation_and_provides_stream_with_wrong_name(): void
    {
        $this->expectException(StreamNotFound::class);

        $stream = $this->getTestStream();

        $this->eventStore->create($stream);

        $this->eventStore->getActionEventEmitter()->attachListener(
            'load',
            function (ActionEvent $event): void {
                $event->setParam('stream', new Stream(new StreamName('EmptyStream'), new ArrayIterator()));
                $event->stopPropagation(true);
            },
            1000
        );

        $this->eventStore->load(new StreamName('user'));
    }

    /**
     * @test
     */
    public function it_uses_stream_provided_by_listener_when_listener_stops_propagation(): void
    {
        $this->expectException(StreamNotFound::class);

        $stream = $this->getTestStream();

        $this->eventStore->create($stream);

        $this->eventStore->getActionEventEmitter()->attachListener(
            'load',
            function (ActionEvent $event): void {
                $event->setParam('stream', new Stream(new StreamName('stream'), new ArrayIterator()));
                $event->stopPropagation(true);
            },
            1000
        );

        $this->eventStore->load($stream->streamName());
    }

    /**
     * @test
     */
    public function it_throws_stream_not_found_exception_if_adapter_loads_nothing(): void
    {
        $this->expectException(StreamNotFound::class);

        $stream = $this->getTestStream();

        $this->eventStore->load($stream->streamName());
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

        $stream = $this->eventStore->load($stream->streamName(), 1, null, $metadataMatcher);

        $this->assertCount(1, $stream->streamEvents());
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
    public function it_appends_empty_stream(): void
    {
        $this->eventStore->appendTo(new StreamName('something'), new ArrayIterator());
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

    protected function getTestStream(): Stream
    {
        $streamEvent = UserCreated::with(
            ['name' => 'Max Mustermann', 'email' => 'contact@prooph.de'],
            1
        );

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');

        return new Stream(new StreamName('Prooph\Model\User'), new ArrayIterator([$streamEvent]));
    }
}
