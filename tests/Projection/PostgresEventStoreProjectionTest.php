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

namespace ProophTest\EventStore\Projection;

use ArrayIterator;
use Prooph\Common\Messaging\Message;
use Prooph\EventStore\Exception\RuntimeException;
use Prooph\EventStore\Exception\StreamNotFound;
use Prooph\EventStore\PDO\Projection\PostgresEventStoreProjection;
use Prooph\EventStore\Projection\InMemoryEventStoreProjection;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\Mock\UsernameChanged;
use ProophTest\EventStore\PDO\Projection\AbstractPostgresEventStoreProjectionTest;
use ProophTest\EventStore\TestCase;

/**
 * @group pdo_pgsql
 */
class PostgresEventStoreProjectionTest extends AbstractPostgresEventStoreProjectionTest
{
    /**
     * @test
     */
    public function it_links_to_and_loads_and_continues_again(): void
    {
        $this->prepareEventStream('user-123');
        $this->eventStore->create(new Stream(new StreamName('foo'), new ArrayIterator()));

        $projection = new PostgresEventStoreProjection(
            $this->eventStore,
            $this->connection,
            'event_streams',
            'projections',
            'test_projection',
            true
        );

        $projection
            ->fromStream('user-123')
            ->whenAny(
                function (array $state, Message $event): array {
                    $this->linkTo('foo', $event);
                    return $state;
                }
            )
            ->run();

        $streams = $this->eventStore->load(new StreamName('foo'));
        $events = $streams->streamEvents();

        $this->assertCount(50, $events);

        $events = [];
        for ($i = 51; $i < 100; $i++) {
            $events[] = UsernameChanged::with([
                'name' => uniqid('name_')
            ], $i);
        }
        $events[] = UsernameChanged::with([
            'name' => 'Oliver'
        ], 100);

        $this->eventStore->appendTo(new StreamName('user-123'), new ArrayIterator($events));

        $projection = new PostgresEventStoreProjection(
            $this->eventStore,
            $this->connection,
            'event_streams',
            'projections',
            'test_projection',
            true
        );

        $projection
            ->fromStream('user-123')
            ->whenAny(
                function (array $state, Message $event): array {
                    $this->linkTo('foo', $event);
                    return $state;
                }
            )
            ->run();

        $streams = $this->eventStore->load(new StreamName('foo'));
        $events = $streams->streamEvents();

        $this->assertCount(100, $events);
    }

    /**
     * @test
     */
    public function it_emits_events_and_resets(): void
    {
        $this->prepareEventStream('user-123');

        $projection = new PostgresEventStoreProjection(
            $this->eventStore,
            $this->connection,
            'event_streams',
            'projections',
            'test_projection',
            true
        );

        $projection
            ->fromStream('user-123')
            ->when([
                UserCreated::class => function (array $state, UserCreated $event): void {
                    $this->emit($event);
                }
            ])
            ->run();

        $streams = $this->eventStore->load(new StreamName('test_projection'));
        $events = $streams->streamEvents();

        $this->assertCount(1, $events);
        $this->assertEquals('Alex', $events->current()->payload()['name']);

        $projection->reset();
        $this->assertEquals('test_projection', $projection->getName());

        $this->expectException(StreamNotFound::class);
        $this->eventStore->load(new StreamName('test_projection'));
    }

    /**
     * @test
     */
    public function it_emits_events_and_deletes(): void
    {
        $this->prepareEventStream('user-123');

        $projection = new PostgresEventStoreProjection(
            $this->eventStore,
            $this->connection,
            'event_streams',
            'projections',
            'test_projection',
            true
        );

        $projection
            ->fromStream('user-123')
            ->when([
                UserCreated::class => function (array $state, UserCreated $event): array {
                    $this->emit($event);
                    return $state;
                }
            ])
            ->run();

        $streams = $this->eventStore->load(new StreamName('test_projection'));
        $events = $streams->streamEvents();

        $this->assertCount(1, $events);
        $this->assertEquals('Alex', $events->current()->payload()['name']);

        $projection->delete(true);

        $this->expectException(StreamNotFound::class);
        $this->eventStore->load(new StreamName('test_projection'));
    }

    /**
     * @test
     */
    public function it_doesnt_emits_events_when_disabled(): void
    {
        $this->expectException(RuntimeException::class);

        $this->prepareEventStream('user-123');

        $projection = new PostgresEventStoreProjection(
            $this->eventStore,
            $this->connection,
            'event_streams',
            'projections',
            'test_projection',
            false
        );

        $projection
            ->fromStream('user-123')
            ->whenAny(function (array $state, Message $event): void {
                $this->emit($event);
            })
            ->run();
    }

    /**
     * @test
     */
    public function it_throws_exception_on_run_when_nothing_configured(): void
    {
        $this->expectException(RuntimeException::class);

        $projection = new PostgresEventStoreProjection(
            $this->eventStore,
            $this->connection,
            'event_streams',
            'projections',
            'test_projection',
            false
        );

        $projection->run();
    }
}
