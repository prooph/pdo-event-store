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

namespace ProophTest\EventStore\Pdo\Projection;

use ArrayIterator;
use PDO;
use PHPUnit\Framework\TestCase;
use Prooph\Common\Messaging\Message;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\Exception\InvalidArgumentException;
use Prooph\EventStore\Exception\RuntimeException;
use Prooph\EventStore\Exception\StreamNotFound;
use Prooph\EventStore\Pdo\Projection\PdoEventStoreProjection;
use Prooph\EventStore\Projection\ProjectionManager;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\Mock\UsernameChanged;

abstract class PdoEventStoreProjectionTest extends TestCase
{
    /**
     * @var ProjectionManager
     */
    protected $projectionManager;

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

    /**
     * @test
     */
    public function it_updates_state_using_when_and_persists_with_block_size(): void
    {
        $this->prepareEventStream('user-123');

        $testCase = $this;

        $projection = $this->projectionManager->createProjection('test_projection', [
            $this->projectionManager::OPTION_PERSIST_BLOCK_SIZE => 10,
        ]);

        $projection
            ->fromAll()
            ->when([
                UserCreated::class => function ($state, Message $event) use ($testCase): array {
                    $testCase->assertEquals('user-123', $this->streamName());
                    $state['name'] = $event->payload()['name'];

                    return $state;
                },
                UsernameChanged::class => function ($state, Message $event) use ($testCase): array {
                    $testCase->assertEquals('user-123', $this->streamName());
                    $state['name'] = $event->payload()['name'];

                    if ($event->payload()['name'] === 'Sascha') {
                        $this->stop();
                    }

                    return $state;
                },
            ])
            ->run();

        $this->assertEquals('Sascha', $projection->getState()['name']);
    }

    /**
     * @test
     */
    public function it_ignores_error_on_delete_of_not_created_stream_projections(): void
    {
        $this->prepareEventStream('user-123');

        $projection = $this->projectionManager->createProjection('test_projection');

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

        $this->assertFalse($this->connection->query('SELECT * FROM projections WHERE real_stream_name = \'user-123\''));
    }

    /**
     * @test
     */
    public function it_throws_exception_when_trying_to_run_two_projections_at_the_same_time(): void
    {
        $this->expectException(\Prooph\EventStore\Exception\RuntimeException::class);
        $this->expectExceptionMessage('Another projection process is already running');

        $this->prepareEventStream('user-123');

        $projection = $this->projectionManager->createProjection('test_projection');

        $projectionManager = $this->projectionManager;

        $projection
            ->fromStream('user-123')
            ->whenAny(
                function (array $state, Message $event) use ($projectionManager): array {
                    $projection = $projectionManager->createProjection('test_projection');

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

        $projection = $this->projectionManager->createProjection('test_projection');

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
    public function it_throws_exception_when_unknown_event_store_instance_passed(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $eventStore = $this->prophesize(EventStore::class);
        $connection = $this->prophesize(\PDO::class);

        new PdoEventStoreProjection(
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

    /**
     * @test
     */
    public function it_deletes_when_projection_before_start_when_it_was_deleted_from_outside(): void
    {
        $this->prepareEventStream('user-123');

        $calledTimes = 0;

        $projection = $this->projectionManager->createProjection('test_projection', [
            $this->projectionManager::OPTION_PERSIST_BLOCK_SIZE => 10,
        ]);

        $projection
            ->init(function (): array {
                return ['count' => 0];
            })
            ->fromStream('user-123')
            ->when([
                UsernameChanged::class => function (array $state, UsernameChanged $event) use (&$calledTimes): array {
                    $state['count']++;
                    $calledTimes++;

                    return $state;
                },
            ])
            ->run(false);

        $events = [];
        for ($i = 51; $i <= 100; $i++) {
            $events[] = UsernameChanged::with([
                'name' => uniqid('name_'),
            ], $i);
        }

        $this->eventStore->appendTo(new StreamName('user-123'), new ArrayIterator($events));

        $this->projectionManager->deleteProjection('test_projection', false);

        $projection->run(false);

        $this->assertEquals(0, $projection->getState()['count']);
        $this->assertEquals(49, $calledTimes);
    }

    /**
     * @test
     */
    public function it_deletes_projection_during_run_when_it_was_deleted_from_outside(): void
    {
        $this->prepareEventStream('user-123');

        $calledTimes = 0;

        $projectionManager = $this->projectionManager;

        $projection = $this->projectionManager->createProjection('test_projection', [
            $this->projectionManager::OPTION_PERSIST_BLOCK_SIZE => 5,
        ]);

        $projection
            ->init(function (): array {
                return ['count' => 0];
            })
            ->fromStream('user-123')
            ->when([
                UsernameChanged::class => function (array $state, UsernameChanged $event) use (&$calledTimes, $projectionManager): array {
                    static $wasReset = false;

                    if (! $wasReset) {
                        $projectionManager->deleteProjection('test_projection', false);
                        $wasReset = true;
                    }

                    $state['count']++;
                    $calledTimes++;

                    return $state;
                },
            ])
            ->run(false);

        $projection->run(false);

        $this->assertEquals(0, $projection->getState()['count']);
        $this->assertEquals(49, $calledTimes);
    }

    /**
     * @test
     */
    public function it_resets_projection_before_start_when_it_was_reset_from_outside(): void
    {
        $this->prepareEventStream('user-123');

        $calledTimes = 0;

        $projection = $this->projectionManager->createProjection('test_projection', [
            $this->projectionManager::OPTION_PERSIST_BLOCK_SIZE => 5,
        ]);

        $projection
            ->init(function (): array {
                return ['count' => 0];
            })
            ->fromStream('user-123')
            ->when([
                UsernameChanged::class => function (array $state, UsernameChanged $event) use (&$calledTimes): array {
                    $state['count']++;
                    $calledTimes++;

                    return $state;
                },
            ])
            ->run(false);

        $events = [];
        for ($i = 51; $i <= 100; $i++) {
            $events[] = UsernameChanged::with([
                'name' => uniqid('name_'),
            ], $i);
        }

        $this->eventStore->appendTo(new StreamName('user-123'), new ArrayIterator($events));

        $this->projectionManager->resetProjection('test_projection');

        $projection->run(false);

        $this->assertEquals(99, $projection->getState()['count']);
        $this->assertEquals(148, $calledTimes);
    }

    /**
     * @test
     */
    public function it_resets_projection_during_run_when_it_was_reset_from_outside(): void
    {
        $this->prepareEventStream('user-123');

        $calledTimes = 0;

        $projectionManager = $this->projectionManager;

        $projection = $this->projectionManager->createProjection('test_projection', [
            $this->projectionManager::OPTION_PERSIST_BLOCK_SIZE => 5,
        ]);

        $projection
            ->init(function (): array {
                return ['count' => 0];
            })
            ->fromStream('user-123')
            ->when([
                UsernameChanged::class => function (array $state, UsernameChanged $event) use (&$calledTimes, $projectionManager): array {
                    static $wasReset = false;

                    if (! $wasReset) {
                        $projectionManager->resetProjection('test_projection');
                        $wasReset = true;
                    }

                    $state['count']++;
                    $calledTimes++;

                    return $state;
                },
            ])
            ->run(false);

        $projection->run(false);

        $this->assertEquals(49, $projection->getState()['count']);
        $this->assertEquals(98, $calledTimes);
    }

    /**
     * @test
     */
    public function it_stops_when_projection_before_start_when_it_was_stopped_from_outside(): void
    {
        $this->prepareEventStream('user-123');

        $calledTimes = 0;

        $projection = $this->projectionManager->createProjection('test_projection', [
            $this->projectionManager::OPTION_PERSIST_BLOCK_SIZE => 5,
        ]);

        $projection
            ->init(function (): array {
                return ['count' => 0];
            })
            ->fromStream('user-123')
            ->when([
                UsernameChanged::class => function (array $state, UsernameChanged $event) use (&$calledTimes): array {
                    $state['count']++;
                    $calledTimes++;

                    return $state;
                },
            ])
            ->run(false);

        $events = [];
        for ($i = 51; $i <= 100; $i++) {
            $events[] = UsernameChanged::with([
                'name' => uniqid('name_'),
            ], $i);
        }

        $this->eventStore->appendTo(new StreamName('user-123'), new ArrayIterator($events));

        $this->projectionManager->stopProjection('test_projection');

        $projection->run(false);

        $this->assertEquals(49, $projection->getState()['count']);
        $this->assertEquals(49, $calledTimes);
    }

    /**
     * @test
     */
    public function it_stops_projection_during_run_when_it_was_stopped_from_outside(): void
    {
        $this->prepareEventStream('user-123');

        $calledTimes = 0;

        $projectionManager = $this->projectionManager;

        $projection = $this->projectionManager->createProjection('test_projection', [
            $this->projectionManager::OPTION_PERSIST_BLOCK_SIZE => 5,
        ]);

        $projection
            ->init(function (): array {
                return ['count' => 0];
            })
            ->fromStream('user-123')
            ->when([
                UsernameChanged::class => function (array $state, UsernameChanged $event) use (&$calledTimes, $projectionManager): array {
                    static $wasReset = false;

                    if (! $wasReset) {
                        $projectionManager->stopProjection('test_projection');
                        $wasReset = true;
                    }

                    $state['count']++;
                    $calledTimes++;

                    return $state;
                },
            ])
            ->run(false);

        $projection->run(false);

        $this->assertEquals(49, $projection->getState()['count']);
        $this->assertEquals(49, $calledTimes);
    }
}
