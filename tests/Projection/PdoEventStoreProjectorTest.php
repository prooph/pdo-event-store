<?php

/**
 * This file is part of prooph/pdo-event-store.
 * (c) 2016-2018 prooph software GmbH <contact@prooph.de>
 * (c) 2016-2018 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace ProophTest\EventStore\Pdo\Projection;

use PDO;
use Prooph\Common\Messaging\Message;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\EventStoreDecorator;
use Prooph\EventStore\Exception\InvalidArgumentException;
use Prooph\EventStore\Pdo\Projection\PdoEventStoreProjector;
use Prooph\EventStore\Projection\ProjectionManager;
use Prooph\EventStore\Projection\Projector;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;
use Prooph\EventStore\Exception\StreamExistsAlready;
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\Mock\UsernameChanged;
use ProophTest\EventStore\Pdo\TestUtil;
use ProophTest\EventStore\Projection\AbstractEventStoreProjectorTest;

abstract class PdoEventStoreProjectorTest extends AbstractEventStoreProjectorTest
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
        TestUtil::tearDownDatabase();
    }

    /**
     * @test
     */
    public function it_updates_state_using_when_and_persists_with_block_size(): void
    {
        $this->prepareEventStream('user-123');

        $testCase = $this;

        $projection = $this->projectionManager->createProjection('test_projection', [
            Projector::OPTION_PERSIST_BLOCK_SIZE => 10,
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
    public function it_handles_existing_projection_table(): void
    {
        $this->prepareEventStream('user-123');

        $projection = $this->projectionManager->createProjection('test_projection');

        $projection
            ->init(function (): array {
                return ['count' => 0];
            })
            ->fromAll()
            ->when([
                UsernameChanged::class => function (array $state, Message $event): array {
                    $state['count']++;

                    return $state;
                },
            ])
            ->run(false);

        $this->assertEquals(49, $projection->getState()['count']);

        $projection->run(false);
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

        new PdoEventStoreProjector(
            $wrappedEventStore->reveal(),
            $this->connection,
            'test_projection',
            'event_streams',
            'projections',
            1,
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

        new PdoEventStoreProjector(
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
    public function it_dispatches_pcntl_signals_when_enabled(): void
    {
        if (! \extension_loaded('pcntl')) {
            $this->markTestSkipped('The PCNTL extension is not available.');

            return;
        }

        $command = 'exec php ' . \realpath(__DIR__) . '/isolated-projection.php';
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
     */
    public function it_respects_update_lock_threshold(): void
    {
        if (! \extension_loaded('pcntl')) {
            $this->markTestSkipped('The PCNTL extension is not available.');

            return;
        }

        $this->prepareEventStream('user-123');

        $command = 'exec php ' . \realpath(__DIR__) . '/isolated-projection.php';
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

        \sleep(1);

        $lockedUntil = TestUtil::getProjectionLockedUntilFromDefaultProjectionsTable($this->connection, 'test_projection');

        $this->assertNotNull($lockedUntil);

        //Update lock threshold is set to 2000 ms
        \usleep(500000);

        $notUpdatedLockedUntil = TestUtil::getProjectionLockedUntilFromDefaultProjectionsTable($this->connection, 'test_projection');

        $this->assertEquals($lockedUntil, $notUpdatedLockedUntil);

        //Now we should definitely see an updated lock
        \sleep(2);

        $processDetails = \proc_get_status($projectionProcess);

        $updatedLockedUntil = TestUtil::getProjectionLockedUntilFromDefaultProjectionsTable($this->connection, 'test_projection');

        $this->assertGreaterThan($lockedUntil, $updatedLockedUntil);

        \posix_kill($processDetails['pid'], SIGQUIT);

        \sleep(1);

        $processDetails = \proc_get_status($projectionProcess);
        $this->assertFalse(
            $processDetails['running']
        );
    }

    /**
     * @test
     */
    public function it_should_update_lock_if_projection_is_not_locked()
    {
        $projectorRef = new \ReflectionClass(PdoEventStoreProjector::class);

        $shouldUpdateLock = new \ReflectionMethod(PdoEventStoreProjector::class, 'shouldUpdateLock');

        $shouldUpdateLock->setAccessible(true);

        $projector = $projectorRef->newInstanceWithoutConstructor();

        $this->assertTrue($shouldUpdateLock->invoke($projector, new \DateTimeImmutable('now', new \DateTimeZone('UTC'))));
    }

    /**
     * @test
     */
    public function it_should_update_lock_if_update_lock_threshold_is_set_to_0()
    {
        $projectorRef = new \ReflectionClass(PdoEventStoreProjector::class);

        $shouldUpdateLock = new \ReflectionMethod(PdoEventStoreProjector::class, 'shouldUpdateLock');

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
    public function it_should_update_lock_if_now_is_greater_than_last_lock_update_plus_threshold()
    {
        $projectorRef = new \ReflectionClass(PdoEventStoreProjector::class);

        $shouldUpdateLock = new \ReflectionMethod(PdoEventStoreProjector::class, 'shouldUpdateLock');

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
    public function it_should_not_update_lock_if_now_is_lower_than_last_lock_update_plus_threshold()
    {
        $projectorRef = new \ReflectionClass(PdoEventStoreProjector::class);

        $shouldUpdateLock = new \ReflectionMethod(PdoEventStoreProjector::class, 'shouldUpdateLock');

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
     */
    public function it_should_create_projection_stream_on_emit()
    {
        $this->prepareEventStream('user-123');

        $projection = $this->projectionManager->createProjection('test_projection_emit');
        $projection
            ->fromStream('user-123')
            ->when([
                UserCreated::class => function (array $state, Message $event) {
                    $this->emit($event);
                }
            ])
            ->run(false);

        $this->assertTrue($this->eventStore->hasStream(new StreamName('test_projection_emit')));
    }

    /**
     * @test
     */
    public function it_should_not_create_projection_stream_on_emit_if_it_already_exists()
    {
        $this->eventStore->create(new Stream(new StreamName('test_projection_emit2'), new \ArrayIterator([])));

        $this->prepareEventStream('user-123');
        $projection = $this->projectionManager->createProjection('test_projection_emit2');
        $projection
            ->fromStream('user-123')
            ->when([
                UserCreated::class => function (array $state, Message $event) {
                    $this->emit($event);
                }
            ]);

        $streamExistsAlreadyExceptionThrowed = false;
        try {
            $projection->run(false);
        } catch (StreamExistsAlready $e) {
            $streamExistsAlreadyExceptionThrowed = true;
        }

        $this->assertFalse($streamExistsAlreadyExceptionThrowed, "StreamExistsAlready exception should not be throwed");
    }
}
