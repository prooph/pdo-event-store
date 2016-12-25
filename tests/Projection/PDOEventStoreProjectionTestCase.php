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
use Prooph\Common\Messaging\Message;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\Exception\RuntimeException;
use Prooph\EventStore\Exception\StreamNotFound;
use Prooph\EventStore\StreamName;
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\Mock\UsernameChanged;

abstract class PDOEventStoreProjectionTestCase extends ProjectionTestCase
{
    /**
     * @test
     */
    public function it_links_to_and_loads_and_continues_again(): void
    {
        $this->prepareEventStream('user-123');

        $projection = $this->eventStore->createProjection('test_projection');

        $projection
            ->fromStream('user-123')
            ->whenAny(
                function (array $state, Message $event): array {
                    $this->linkTo('foo', $event);

                    if ($event->metadata()['_aggregate_version'] === 50) {
                        $this->stop();
                    }

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
                'name' => uniqid('name_'),
            ], $i);
        }
        $events[] = UsernameChanged::with([
            'name' => 'Oliver',
        ], 100);

        $this->eventStore->appendTo(new StreamName('user-123'), new ArrayIterator($events));

        $projection = $this->eventStore->createProjection('test_projection');

        $projection
            ->fromStream('user-123')
            ->whenAny(
                function (array $state, Message $event): array {
                    $this->linkTo('foo', $event);

                    if ($event->metadata()['_aggregate_version'] === 100) {
                        $this->stop();
                    }

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

        $projection = $this->eventStore->createProjection('test_projection');

        $projection
            ->fromStream('user-123')
            ->when([
                UserCreated::class => function (array $state, UserCreated $event): void {
                    $this->emit($event);
                    $this->stop();
                },
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

        $projection = $this->eventStore->createProjection('test_projection');

        $projection
            ->fromStream('user-123')
            ->when([
                UserCreated::class => function (array $state, UserCreated $event): array {
                    $this->emit($event);
                    $this->stop();

                    return $state;
                },
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
    public function it_ignores_error_on_delete_of_not_created_stream_projections(): void
    {
        $this->prepareEventStream('user-123');

        $projection = $this->eventStore->createProjection('test_projection');

        $projection
            ->fromStream('user-123')
            ->when([
                UserCreated::class => function (array $state, UserCreated $event): array {
                    $this->stop();

                    return $state;
                },
            ])
            ->run();

        $projection->delete(true);
    }

    /**
     * @test
     */
    public function it_throws_exception_on_run_when_nothing_configured(): void
    {
        $this->expectException(RuntimeException::class);

        $projection = $this->eventStore->createProjection('test_projection');

        $projection->run();
    }

    /**
     * @test
     */
    public function it_throws_exception_when_trying_to_run_two_projections_at_the_same_time(): void
    {
        $this->expectException(\Prooph\EventStore\PDO\Exception\RuntimeException::class);
        $this->expectExceptionMessage('Another projection process is already running');

        $this->prepareEventStream('user-123');

        $projection = $this->eventStore->createProjection('test_projection');

        $eventStore = $this->eventStore;

        $projection
            ->fromStream('user-123')
            ->whenAny(
                function (array $state, Message $event) use ($eventStore): array {
                    $projection = $eventStore->createProjection('test_projection');

                    $projection
                        ->fromStream('user-123')
                        ->whenAny(
                            function (array $state, Message $event): array {
                                $this->linkTo('foo', $event);

                                return $state;
                            }
                        )
                        ->run();
                }
            )
            ->run();
    }

    /**
     * @test
     */
    public function it_handles_missing_projection_table(): void
    {
        $this->expectException(\Prooph\EventStore\PDO\Exception\RuntimeException::class);
        $this->expectExceptionMessage('Unknown error. Maybe the projection table is not setup?');

        $this->prepareEventStream('user-123');

        $this->connection->exec('DROP TABLE projections;');

        $projection = $this->eventStore->createProjection('test_projection');

        $projection
            ->fromStream('user-123')
            ->when([
                UserCreated::class => function (array $state, UserCreated $event): array {
                    $this->stop();

                    return $state;
                },
            ])
            ->run();
    }
}
