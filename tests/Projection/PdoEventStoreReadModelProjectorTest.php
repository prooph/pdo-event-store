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
use Prooph\Common\Messaging\Message;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\EventStoreDecorator;
use Prooph\EventStore\Exception\InvalidArgumentException;
use Prooph\EventStore\Pdo\Projection\PdoEventStoreReadModelProjector;
use Prooph\EventStore\Projection\ProjectionManager;
use Prooph\EventStore\Projection\ReadModel;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;
use ProophTest\EventStore\Mock\ReadModelMock;
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\Mock\UsernameChanged;
use ProophTest\EventStore\Projection\AbstractEventStoreReadModelProjectorTest;

abstract class PdoEventStoreReadModelProjectorTest extends AbstractEventStoreReadModelProjectorTest
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
        $this->connection->exec('DROP TABLE IF EXISTS event_streams;');
        $this->connection->exec('DROP TABLE IF EXISTS projections;');
        $this->connection->exec('DROP TABLE IF EXISTS _' . sha1('user-123'));
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
                'name' => uniqid('name_'),
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
     */
    public function it_handles_missing_projection_table(): void
    {
        $this->expectException(\Prooph\EventStore\Exception\RuntimeException::class);
        $this->expectExceptionMessage('Maybe the projection table is not setup?');

        $this->prepareEventStream('user-123');

        $this->connection->exec('DROP TABLE projections;');

        $projection = $this->projectionManager->createReadModelProjection('test_projection', new ReadModelMock());

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
