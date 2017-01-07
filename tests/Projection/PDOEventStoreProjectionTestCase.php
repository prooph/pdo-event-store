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

namespace ProophTest\EventStore\PDO\Projection;

use ArrayIterator;
use PDO;
use PHPUnit\Framework\TestCase;
use Prooph\Common\Event\ProophActionEventEmitter;
use Prooph\Common\Messaging\Message;
use Prooph\EventStore\ActionEventEmitterEventStore;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\Exception\InvalidArgumentException;
use Prooph\EventStore\Exception\RuntimeException;
use Prooph\EventStore\Exception\StreamNotFound;
use Prooph\EventStore\PDO\Projection\PDOEventStoreProjection;
use Prooph\EventStore\PDO\Projection\ProjectionOptions;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\Mock\UsernameChanged;

abstract class PDOEventStoreProjectionTestCase extends TestCase
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
        // these tables are used in every test case
        $this->connection->exec('DROP TABLE event_streams;');
        $this->connection->exec('DROP TABLE projections;');
        $this->connection->exec('DROP TABLE _' . sha1('user-123'));
        // these tables are used only in some test cases
        $this->connection->exec('DROP TABLE IF EXISTS _' . sha1('user-234'));
        $this->connection->exec('DROP TABLE IF EXISTS _' . sha1('$iternal-345'));
        $this->connection->exec('DROP TABLE IF EXISTS _' . sha1('guest-345'));
        $this->connection->exec('DROP TABLE IF EXISTS _' . sha1('guest-456'));
        $this->connection->exec('DROP TABLE IF EXISTS _' . sha1('foo'));
        $this->connection->exec('DROP TABLE IF EXISTS _' . sha1('test_projection'));
    }

    protected function prepareEventStream(string $name): void
    {
        $events = [];
        $events[] = UserCreated::with([
            'name' => 'Alex',
        ], 1);
        for ($i = 2; $i < 50; $i++) {
            $events[] = UsernameChanged::with([
                'name' => uniqid('name_'),
            ], $i);
        }
        $events[] = UsernameChanged::with([
            'name' => 'Sascha',
        ], 50);

        $this->eventStore->create(new Stream(new StreamName($name), new ArrayIterator($events)));
    }

    /**
     * @test
     */
    public function it_unwraps_event_store_decorator(): void
    {
        $eventStoreDecorator = new ActionEventEmitterEventStore($this->eventStore, new ProophActionEventEmitter());

        $eventStoreDecorator->createProjection('test_projection', new ProjectionOptions());
    }

    /**
     * @test
     */
    public function it_can_query_from_stream_and_reset()
    {
        $this->prepareEventStream('user-123');

        $projection = $this->eventStore->createProjection('test_projection');

        $projection
            ->init(function (): array {
                return ['count' => 0];
            })
            ->fromStream('user-123')
            ->when([
                UsernameChanged::class => function (array $state, UsernameChanged $event): array {
                    $state['count']++;

                    return $state;
                },
            ])
            ->run(false);

        $this->assertEquals(49, $projection->getState()['count']);

        $projection->reset();

        $projection->run(false);

        $this->assertEquals(49, $projection->getState()['count']);
    }

    /**
     * @test
     */
    public function it_can_query_from_streams(): void
    {
        $this->prepareEventStream('user-123');
        $this->prepareEventStream('user-234');

        $projection = $this->eventStore->createProjection('test_projection');

        $projection
            ->init(function (): array {
                return ['count' => 0];
            })
            ->fromStreams('user-123', 'user-234')
            ->whenAny(
                function (array $state, Message $event): array {
                    $state['count']++;

                    return $state;
                }
            )
            ->run(false);

        $this->assertEquals(100, $projection->getState()['count']);
    }

    /**
     * @test
     */
    public function it_can_query_from_all_ignoring_internal_streams(): void
    {
        $this->prepareEventStream('user-123');
        $this->prepareEventStream('user-234');
        $this->prepareEventStream('$iternal-345');

        $projection = $this->eventStore->createProjection('test_projection');

        $projection
            ->init(function (): array {
                return ['count' => 0];
            })
            ->fromAll()
            ->whenAny(
                function (array $state, Message $event): array {
                    $state['count']++;

                    return $state;
                }
            )
            ->run(false);

        $this->assertEquals(100, $projection->getState()['count']);
    }

    /**
     * @test
     */
    public function it_can_query_from_category_with_when_all()
    {
        $this->prepareEventStream('user-123');
        $this->prepareEventStream('user-234');

        $projection = $this->eventStore->createProjection('test_projection');

        $projection
            ->init(function (): array {
                return ['count' => 0];
            })
            ->fromCategory('user')
            ->whenAny(
                function (array $state, Message $event): array {
                    $state['count']++;

                    return $state;
                }
            )
            ->run(false);

        $this->assertEquals(100, $projection->getState()['count']);
    }

    /**
     * @test
     */
    public function it_can_query_from_categories_with_when()
    {
        $this->prepareEventStream('user-123');
        $this->prepareEventStream('user-234');
        $this->prepareEventStream('guest-345');
        $this->prepareEventStream('guest-456');

        $projection = $this->eventStore->createProjection('test_projection');

        $projection
            ->init(function (): array {
                return ['count' => 0];
            })
            ->fromCategories('user', 'guest')
            ->when([
                UserCreated::class => function (array $state, Message $event): array {
                    $state['count']++;

                    return $state;
                },
            ])
            ->run(false);

        $this->assertEquals(4, $projection->getState()['count']);
    }

    public function it_resumes_query_from_position(): void
    {
        $this->prepareEventStream('user-123');

        $projection = $this->eventStore->createProjection('test_projection');

        $projection
            ->init(function (): array {
                return ['count' => 0];
            })
            ->fromCategories('user', 'guest')
            ->when([
                UsernameChanged::class => function (array $state, Message $event): array {
                    $state['count']++;

                    return $state;
                },
            ])
            ->run(false);

        $this->assertEquals(49, $projection->getState()['count']);

        $events = [];
        for ($i = 51; $i <= 100; $i++) {
            $events[] = UsernameChanged::with([
                'name' => uniqid('name_'),
            ], $i);
        }

        $this->eventStore->appendTo(new StreamName('user-123'), new ArrayIterator($events));

        $projection->run(false);

        $this->assertEquals(99, $projection->getState()['count']);
    }

    /**
     * @test
     */
    public function it_resets_to_empty_array(): void
    {
        $projection = $this->eventStore->createProjection('test_projection');

        $state = $projection->getState();

        $this->assertInternalType('array', $state);

        $projection->reset();

        $state2 = $projection->getState();

        $this->assertInternalType('array', $state2);
    }

    /**
     * @test
     */
    public function it_can_be_stopped_while_processing()
    {
        $this->prepareEventStream('user-123');

        $projection = $this->eventStore->createProjection('test_projection');

        $projection
            ->init(function (): array {
                return ['count' => 0];
            })
            ->fromStream('user-123')
            ->whenAny(function (array $state, Message $event): array {
                $state['count']++;

                if ($state['count'] === 10) {
                    $this->stop();
                }

                return $state;
            })
            ->run(false);

        $this->assertEquals(10, $projection->getState()['count']);
    }

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
        $this->expectException(\Prooph\EventStore\Exception\RuntimeException::class);
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
        $this->expectException(\Prooph\EventStore\Exception\RuntimeException::class);
        $this->expectExceptionMessage('Maybe the projection table is not setup?');

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

    /**
     * @test
     */
    public function it_throws_exception_when_init_callback_provided_twice(): void
    {
        $this->expectException(RuntimeException::class);

        $projection = $this->eventStore->createProjection('test_projection');

        $projection->init(function (): array {
            return [];
        });
        $projection->init(function (): array {
            return [];
        });
    }

    /**
     * @test
     */
    public function it_throws_exception_when_from_called_twice(): void
    {
        $this->expectException(RuntimeException::class);

        $projection = $this->eventStore->createProjection('test_projection');

        $projection->fromStream('foo');
        $projection->fromStream('bar');
    }

    /**
     * @test
     */
    public function it_throws_exception_when_from_called_twice_2(): void
    {
        $this->expectException(RuntimeException::class);

        $projection = $this->eventStore->createProjection('test_projection');

        $projection->fromStreams('foo');
        $projection->fromCategory('bar');
    }

    /**
     * @test
     */
    public function it_throws_exception_when_from_called_twice_3(): void
    {
        $this->expectException(RuntimeException::class);

        $projection = $this->eventStore->createProjection('test_projection');

        $projection->fromCategory('foo');
        $projection->fromStreams('bar');
    }

    /**
     * @test
     */
    public function it_throws_exception_when_from_called_twice_4(): void
    {
        $this->expectException(RuntimeException::class);

        $projection = $this->eventStore->createProjection('test_projection');

        $projection->fromCategories('foo');
        $projection->fromCategories('bar');
    }

    /**
     * @test
     */
    public function it_throws_exception_when_from_called_twice_5(): void
    {
        $this->expectException(RuntimeException::class);

        $projection = $this->eventStore->createProjection('test_projection');

        $projection->fromCategories('foo');
        $projection->fromAll('bar');
    }

    /**
     * @test
     */
    public function it_throws_exception_when_when_called_twice_(): void
    {
        $this->expectException(RuntimeException::class);

        $projection = $this->eventStore->createProjection('test_projection');

        $projection->when(['foo' => function (): void {
        }]);
        $projection->when(['foo' => function (): void {
        }]);
    }

    /**
     * @test
     */
    public function it_throws_exception_when_invalid_handlers_configured(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $projection = $this->eventStore->createProjection('test_projection');

        $projection->when(['1' => function (): void {
        }]);
    }

    /**
     * @test
     */
    public function it_throws_exception_when_invalid_handlers_configured_2(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $projection = $this->eventStore->createProjection('test_projection');

        $projection->when(['foo' => 'invalid']);
    }

    /**
     * @test
     */
    public function it_throws_exception_when_whenAny_called_twice(): void
    {
        $this->expectException(RuntimeException::class);

        $projection = $this->eventStore->createProjection('test_projection');

        $projection->whenAny(function (): void {
        });
        $projection->whenAny(function (): void {
        });
    }

    /**
     * @test
     */
    public function it_throws_exception_when_unknown_event_store_instance_passed(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $eventStore = $this->prophesize(EventStore::class);
        $connection = $this->prophesize(\PDO::class);

        new PDOEventStoreProjection(
            $eventStore->reveal(),
            $connection->reveal(),
            'test_projection',
            'event_streams',
            'projections',
            10,
            10,
            10,
            10000
        );
    }
}
