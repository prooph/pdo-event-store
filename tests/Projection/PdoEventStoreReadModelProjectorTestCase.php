<?php

/**
 * This file is part of prooph/pdo-event-store.
 * (c) 2016-2025 Alexander Miertsch <kontakt@codeliner.ws>
 * (c) 2016-2025 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace ProophTest\EventStore\Pdo\Projection;

use ArrayIterator;
use DateInterval;
use DateTimeImmutable;
use DateTimeZone;
use PDO;
use Prooph\Common\Messaging\Message;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\EventStoreDecorator;
use Prooph\EventStore\Exception\InvalidArgumentException;
use Prooph\EventStore\Pdo\Projection\GapDetection;
use Prooph\EventStore\Pdo\Projection\PdoEventStoreProjector;
use Prooph\EventStore\Pdo\Projection\PdoEventStoreReadModelProjector;
use Prooph\EventStore\Projection\ProjectionManager;
use Prooph\EventStore\Projection\Projector;
use Prooph\EventStore\Projection\ReadModel;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;
use ProophTest\EventStore\Mock\ReadModelMock;
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\Mock\UsernameChanged;
use ProophTest\EventStore\Pdo\TestUtil;
use ProophTest\EventStore\Projection\AbstractEventStoreReadModelProjectorTest;
use Prophecy\PhpUnit\ProphecyTrait;

abstract class PdoEventStoreReadModelProjectorTestCase extends AbstractEventStoreReadModelProjectorTest
{
    use ProphecyTrait;

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

    abstract protected function setUpEventStoreWithControlledConnection(PDO $connection): EventStore;

    protected function tearDown(): void
    {
        TestUtil::tearDownDatabase();
    }

    protected function prepareEventStream(string $name): void
    {
        $events = [];
        $events[] = UserCreated::with([
            'name' => 'Alex',
        ], 1);
        for ($i = 2; $i < 50; $i++) {
            $events[] = UsernameChanged::with([
                'name' => \uniqid('name_'),
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
    public function it_updates_read_model_using_when_and_loads_and_continues_again(): void
    {
        $this->prepareEventStream('user-123');

        $readModel = new ReadModelMock();

        $projection = $this->projectionManager->createReadModelProjection('test_projection', $readModel);

        $projection
            ->fromAll()
            ->when([
                UserCreated::class => function ($state, Message $event): void {
                    $this->readModel()->stack('insert', 'name', $event->payload()['name']);
                },
                UsernameChanged::class => function ($state, Message $event): void {
                    $this->readModel()->stack('update', 'name', $event->payload()['name']);

                    if ($event->metadata()['_aggregate_version'] === 50) {
                        $this->stop();
                    }
                },
            ])
            ->run();

        $this->assertEquals('Sascha', $readModel->read('name'));

        $events = [];
        for ($i = 51; $i < 100; $i++) {
            $events[] = UsernameChanged::with([
                'name' => \uniqid('name_'),
            ], $i);
        }
        $events[] = UsernameChanged::with([
            'name' => 'Oliver',
        ], 100);

        $this->eventStore->appendTo(new StreamName('user-123'), new ArrayIterator($events));

        $projection = $this->projectionManager->createReadModelProjection('test_projection', $readModel);

        $projection
            ->fromAll()
            ->when([
                UserCreated::class => function ($state, Message $event): void {
                    $this->readModel()->stack('insert', 'name', $event->payload()['name']);
                },
                UsernameChanged::class => function ($state, Message $event): void {
                    $this->readModel()->stack('update', 'name', $event->payload()['name']);

                    if ($event->metadata()['_aggregate_version'] === 100) {
                        $this->stop();
                    }
                },
            ])
            ->run();

        $this->assertEquals('Oliver', $readModel->read('name'));

        $projection->reset();

        $this->assertFalse($readModel->hasKey('name'));
    }

    /**
     * @test
     */
    public function it_throws_exception_when_invalid_wrapped_event_store_instance_passed(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Unknown event store instance given');

        $eventStore = $this->prophesize(EventStore::class);
        $wrappedEventStore = $this->prophesize(EventStoreDecorator::class);
        $wrappedEventStore->getInnerEventStore()->willReturn($eventStore->reveal())->shouldBeCalled();

        new PdoEventStoreReadModelProjector(
            $wrappedEventStore->reveal(),
            $this->connection,
            'test_projection',
            new ReadModelMock(),
            'event_streams',
            'projections',
            1,
            1,
            1
        );
    }

    /**
     * @test
     */
    public function it_throws_exception_when_unknown_event_store_instance_passed(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Unknown event store instance given');

        $eventStore = $this->prophesize(EventStore::class);
        $connection = $this->prophesize(PDO::class);
        $readModel = $this->prophesize(ReadModel::class);

        new PdoEventStoreReadModelProjector(
            $eventStore->reveal(),
            $connection->reveal(),
            'test_projection',
            $readModel->reveal(),
            'event_streams',
            'projections',
            10,
            10,
            10
        );
    }

    /**
     * @test
     * @medium
     */
    public function it_dispatches_pcntl_signals_when_enabled(): void
    {
        if (! \extension_loaded('pcntl')) {
            $this->markTestSkipped('The PCNTL extension is not available.');

            return;
        }

        $command = 'exec php ' . \realpath(__DIR__) . '/isolated-read-model-projection.php';
        $descriptorSpec = [
            0 => ['pipe', 'r'],
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ];
        /**
         * Created process inherits env variables from this process.
         * Script returns with non-standard code SIGUSR1 from the handler and -1 else
         */
        $projectionProcess = \proc_open($command, $descriptorSpec, $pipes);
        $processDetails = \proc_get_status($projectionProcess);
        \sleep(1);
        \posix_kill($processDetails['pid'], SIGQUIT);
        \sleep(1);

        $processDetails = \proc_get_status($projectionProcess);
        $this->assertEquals(
            SIGUSR1,
            $processDetails['exitcode']
        );
    }

    /**
     * @test
     * @large
     */
    public function it_respects_update_lock_threshold(): void
    {
        if (! \extension_loaded('pcntl')) {
            $this->markTestSkipped('The PCNTL extension is not available.');

            return;
        }

        $this->prepareEventStream('user-123');

        $command = 'exec php ' . \realpath(__DIR__) . '/isolated-read-model-projection.php';
        $descriptorSpec = [
            0 => ['pipe', 'r'],
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ];
        /**
         * Created process inherits env variables from this process.
         * Script returns with non-standard code SIGUSR1 from the handler and -1 else
         */
        $projectionProcess = \proc_open($command, $descriptorSpec, $pipes);
        $processDetails = \proc_get_status($projectionProcess);

        \sleep(1);

        $lockedUntil = TestUtil::getProjectionLockedUntilFromDefaultProjectionsTable($this->connection, 'test_projection');

        $this->assertNotNull($lockedUntil);

        //Update lock threshold is set to 2000 ms
        \usleep(500000);

        $notUpdatedLockedUntil = TestUtil::getProjectionLockedUntilFromDefaultProjectionsTable($this->connection, 'test_projection');

        $this->assertEquals($lockedUntil, $notUpdatedLockedUntil);

        //Now we should definitely see an updated lock
        \sleep(2);

        $updatedLockedUntil = TestUtil::getProjectionLockedUntilFromDefaultProjectionsTable($this->connection, 'test_projection');

        $this->assertGreaterThan($lockedUntil, $updatedLockedUntil);

        \posix_kill($processDetails['pid'], SIGQUIT);

        \sleep(1);

        $processDetails = \proc_get_status($projectionProcess);
        $this->assertEquals(
            SIGUSR1,
            $processDetails['exitcode']
        );
    }

    /**
     * @test
     */
    public function it_should_update_lock_if_projection_is_not_locked(): void
    {
        $projectorRef = new \ReflectionClass(PdoEventStoreReadModelProjector::class);

        $shouldUpdateLock = new \ReflectionMethod(PdoEventStoreReadModelProjector::class, 'shouldUpdateLock');

        $shouldUpdateLock->setAccessible(true);

        $projector = $projectorRef->newInstanceWithoutConstructor();

        $this->assertTrue($shouldUpdateLock->invoke($projector, new \DateTimeImmutable('now', new \DateTimeZone('UTC'))));
    }

    /**
     * @test
     */
    public function it_should_update_lock_if_update_lock_threshold_is_set_to_0(): void
    {
        $projectorRef = new \ReflectionClass(PdoEventStoreReadModelProjector::class);

        $shouldUpdateLock = new \ReflectionMethod(PdoEventStoreReadModelProjector::class, 'shouldUpdateLock');

        $shouldUpdateLock->setAccessible(true);

        $projector = $projectorRef->newInstanceWithoutConstructor();

        $now = new \DateTimeImmutable('now', new \DateTimeZone('UTC'));

        $lastLockUpdateProp = $projectorRef->getProperty('lastLockUpdate');
        $lastLockUpdateProp->setAccessible(true);
        $lastLockUpdateProp->setValue($projector, $now);

        $updateLockThresholdProp = $projectorRef->getProperty('updateLockThreshold');
        $updateLockThresholdProp->setAccessible(true);
        $updateLockThresholdProp->setValue($projector, 0);

        $this->assertTrue($shouldUpdateLock->invoke($projector, $now));
    }

    /**
     * @test
     */
    public function it_should_update_lock_if_now_is_greater_than_last_lock_update_plus_threshold(): void
    {
        $projectorRef = new \ReflectionClass(PdoEventStoreReadModelProjector::class);

        $shouldUpdateLock = new \ReflectionMethod(PdoEventStoreReadModelProjector::class, 'shouldUpdateLock');

        $shouldUpdateLock->setAccessible(true);

        $projector = $projectorRef->newInstanceWithoutConstructor();

        $now = new \DateTimeImmutable('now', new \DateTimeZone('UTC'));

        $lastLockUpdateProp = $projectorRef->getProperty('lastLockUpdate');
        $lastLockUpdateProp->setAccessible(true);
        $lastLockUpdateProp->setValue($projector, TestUtil::subMilliseconds($now, 800));

        $updateLockThresholdProp = $projectorRef->getProperty('updateLockThreshold');
        $updateLockThresholdProp->setAccessible(true);
        $updateLockThresholdProp->setValue($projector, 500);

        $this->assertTrue($shouldUpdateLock->invoke($projector, $now));
    }

    /**
     * @test
     */
    public function it_should_not_update_lock_if_now_is_lower_than_last_lock_update_plus_threshold(): void
    {
        $projectorRef = new \ReflectionClass(PdoEventStoreReadModelProjector::class);

        $shouldUpdateLock = new \ReflectionMethod(PdoEventStoreReadModelProjector::class, 'shouldUpdateLock');

        $shouldUpdateLock->setAccessible(true);

        $projector = $projectorRef->newInstanceWithoutConstructor();

        $now = new \DateTimeImmutable('now', new \DateTimeZone('UTC'));

        $lastLockUpdateProp = $projectorRef->getProperty('lastLockUpdate');
        $lastLockUpdateProp->setAccessible(true);
        $lastLockUpdateProp->setValue($projector, TestUtil::subMilliseconds($now, 300));

        $updateLockThresholdProp = $projectorRef->getProperty('updateLockThreshold');
        $updateLockThresholdProp->setAccessible(true);
        $updateLockThresholdProp->setValue($projector, 500);

        $this->assertFalse($shouldUpdateLock->invoke($projector, $now));
    }

    /**
     * @test
     * @medium
     * @testWith        [1, 70]
     *                  [20, 70]
     *                  [21, 71]
     */
    public function a_running_projector_that_is_reset_should_run_again(int $blockSize, int $expectedEventsCount): void
    {
        $this->prepareEventStream('user-123');

        $projection = $this->projectionManager->createReadModelProjection('test_projection', new ReadModelMock(), [
            Projector::OPTION_PERSIST_BLOCK_SIZE => $blockSize,
        ]);

        $projectionManager = $this->projectionManager;

        $eventCounter = 0;

        $projection
            ->fromAll()
            ->when([
                UserCreated::class => function ($state, Message $event) use (&$eventCounter): void {
                    $eventCounter++;
                },
                UsernameChanged::class => function ($state, Message $event) use (&$eventCounter, $projectionManager): void {
                    $eventCounter++;

                    if ($eventCounter === 20) {
                        $projectionManager->resetProjection('test_projection');
                    }

                    if ($eventCounter === 70) {
                        $projectionManager->stopProjection('test_projection');
                    }
                },
            ])
            ->run(true);

        $this->assertSame($expectedEventsCount, $eventCounter);
    }

    /**
     * @test
     */
    public function not_acting_on_all_events_should_yield_correct_stream_position(): void
    {
        $this->prepareEventStream($sStreamName = 'user');

        $projectionManager = $this->projectionManager;
        $projection = $projectionManager->createReadModelProjection('test_projection', new ReadModelMock(), [
            Projector::OPTION_PERSIST_BLOCK_SIZE => 1,
        ]);

        $projection
            ->fromStream('user')
            ->init(function () {
                return [];
            })
            ->when([
                UsernameChanged::class => function (array $state, Message $event): array {
                    return $state;
                },
            ])
            ->run(false);

        $this->assertEquals(50, $projectionManager->fetchProjectionStreamPositions('test_projection')['user']);
    }

    /**
     * @test
     */
    public function it_detects_gap_and_performs_retry(): void
    {
        $streamName = new StreamName('user');

        $this->prepareEventStreamWithOneEvent($streamName->toString());

        $parallelConnection = TestUtil::getConnection(false);
        $secondEventStore = $this->setUpEventStoreWithControlledConnection($parallelConnection);

        // Begin transaction and let it open
        $parallelConnection->beginTransaction();

        $secondEventStore->appendTo($streamName, new ArrayIterator([
            UsernameChanged::with([
                'name' => 'Bob',
            ], 2),
        ]));

        // Insert third event, while parallel connection is still in transaction
        $this->eventStore->appendTo($streamName, new ArrayIterator([
            UsernameChanged::with([
                'name' => 'Sascha',
            ], 3),
        ]));

        //Only one immediate retry and an increased detection window to have enough time for the test to succeed
        $gapDetection = new GapDetection([0], new \DateInterval('PT60S'));

        $projectionManager = $this->projectionManager;
        $projection = $projectionManager->createReadModelProjection('test_projection', new ReadModelMock(), [
            Projector::OPTION_PERSIST_BLOCK_SIZE => 1,
            PdoEventStoreProjector::OPTION_GAP_DETECTION => $gapDetection,
        ]);

        $projection
            ->fromStream('user')
            ->init(function () {
                return [];
            })
            ->when([
                UserCreated::class => function (array $state, Message $event): array {
                    return $state;
                },
                UsernameChanged::class => function (array $state, Message $event): array {
                    return $state;
                },
            ])
            ->run(false);

        $this->assertEquals(1, $projectionManager->fetchProjectionStreamPositions('test_projection')['user']);

        $this->assertTrue($gapDetection->isRetrying());

        // Fill the gap
        $parallelConnection->commit();

        // Run again with gap detection in retry mode
        $projection->run(false);

        $this->assertEquals(3, $projectionManager->fetchProjectionStreamPositions('test_projection')['user']);

        $this->assertFalse($gapDetection->isRetrying());
    }

    /**
     * @test
     */
    public function it_continues_when_retry_limit_is_reached_and_gap_not_filled(): void
    {
        $streamName = new StreamName('user');

        $this->prepareEventStreamWithOneEvent($streamName->toString());

        $parallelConnection = TestUtil::getConnection(false);
        $secondEventStore = $this->setUpEventStoreWithControlledConnection($parallelConnection);

        // Begin transaction and let it open
        $parallelConnection->beginTransaction();

        $secondEventStore->appendTo($streamName, new ArrayIterator([
            UsernameChanged::with([
                'name' => 'Bob',
            ], 2),
        ]));

        // Insert third event, while parallel connection is still in transaction
        $this->eventStore->appendTo($streamName, new ArrayIterator([
            UsernameChanged::with([
                'name' => 'Sascha',
            ], 3),
        ]));

        //Two retries and an increased detection window to have enough time for the test to succeed
        $gapDetection = new GapDetection([0, 5], new \DateInterval('PT60S'));

        $projectionManager = $this->projectionManager;
        $projection = $projectionManager->createReadModelProjection('test_projection', new ReadModelMock(), [
            Projector::OPTION_PERSIST_BLOCK_SIZE => 1,
            PdoEventStoreProjector::OPTION_GAP_DETECTION => $gapDetection,
        ]);

        $projection
            ->fromStream('user')
            ->init(function () {
                return [];
            })
            ->when([
                UserCreated::class => function (array $state, Message $event): array {
                    return $state;
                },
                UsernameChanged::class => function (array $state, Message $event): array {
                    return $state;
                },
            ])
            ->run(false);

        $this->assertEquals(1, $projectionManager->fetchProjectionStreamPositions('test_projection')['user']);

        $this->assertTrue($gapDetection->isRetrying());

        // Force a real gap
        $parallelConnection->rollBack();

        // Run again with gap detection in retry mode
        $projection->run(false);

        // Projection should not move forward, but instead retry a second time
        $this->assertEquals(1, $projectionManager->fetchProjectionStreamPositions('test_projection')['user']);

        // Third run with gap detection still in retry mode, but limit reached
        $projection->run(false);

        //Projection should have moved forward
        $this->assertEquals(3, $projectionManager->fetchProjectionStreamPositions('test_projection')['user']);

        $this->assertFalse($gapDetection->isRetrying());
    }

    /**
     * @test
     */
    public function it_does_not_perform_retry_when_event_is_older_than_detection_window(): void
    {
        $streamName = new StreamName('user');

        // Simulate a projection replay by setting createdAt to yesterday
        $timeOfRecording = (new \DateTimeImmutable('now', new DateTimeZone('UTC')))->sub(new DateInterval('P1D'));

        $this->prepareEventStreamWithOneEvent($streamName->toString(), $timeOfRecording);

        $parallelConnection = TestUtil::getConnection(false);
        $secondEventStore = $this->setUpEventStoreWithControlledConnection($parallelConnection);

        // Begin transaction and let it open
        $parallelConnection->beginTransaction();

        $secondEventStore->appendTo($streamName, new ArrayIterator([
            UsernameChanged::withPayloadAndSpecifiedCreatedAt([
                'name' => 'Bob',
            ], 2, $timeOfRecording),
        ]));

        // Insert third event, while parallel connection is still in transaction
        $this->eventStore->appendTo($streamName, new ArrayIterator([
            UsernameChanged::withPayloadAndSpecifiedCreatedAt([
                'name' => 'Sascha',
            ], 3, $timeOfRecording),
        ]));

        //Two retries but detection window set to one minute only
        $gapDetection = new GapDetection([0, 5], new \DateInterval('PT60S'));

        $projectionManager = $this->projectionManager;
        $projection = $projectionManager->createReadModelProjection('test_projection', new ReadModelMock(), [
            Projector::OPTION_PERSIST_BLOCK_SIZE => 1,
            PdoEventStoreProjector::OPTION_GAP_DETECTION => $gapDetection,
        ]);

        $projection
            ->fromStream('user')
            ->init(function () {
                return [];
            })
            ->when([
                UserCreated::class => function (array $state, Message $event): array {
                    return $state;
                },
                UsernameChanged::class => function (array $state, Message $event): array {
                    return $state;
                },
            ])
            ->run(false);

        // No retry, since gap is too old
        $this->assertEquals(3, $projectionManager->fetchProjectionStreamPositions('test_projection')['user']);

        $parallelConnection->rollBack();

        $this->assertFalse($gapDetection->isRetrying());
    }

    protected function prepareEventStreamWithOneEvent(string $name, ?DateTimeImmutable $createdAt = null): void
    {
        $events = [];

        if ($createdAt) {
            $events[] = UserCreated::withPayloadAndSpecifiedCreatedAt([
                'name' => 'Alex',
            ], 1, $createdAt);
        } else {
            $events[] = UserCreated::with([
                'name' => 'Alex',
            ], 1);
        }

        $this->eventStore->create(new Stream(new StreamName($name), new ArrayIterator($events)));
    }
}
