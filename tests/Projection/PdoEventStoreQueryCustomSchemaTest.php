<?php

/**
 * This file is part of prooph/pdo-event-store.
 * (c) 2016-2022 Alexander Miertsch <kontakt@codeliner.ws>
 * (c) 2016-2022 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
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
use Prooph\EventStore\Pdo\Projection\PdoEventStoreQuery;
use Prooph\EventStore\Projection\ProjectionManager;
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\Mock\UsernameChanged;
use ProophTest\EventStore\Pdo\TestUtil;
use ProophTest\EventStore\Projection\AbstractEventStoreQueryTest;
use Prophecy\PhpUnit\ProphecyTrait;

abstract class PdoEventStoreQueryCustomSchemaTest extends AbstractEventStoreQueryTest
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

    protected function tearDown(): void
    {
        TestUtil::tearDownDatabase();
    }

    protected function eventStreamsTable(): string
    {
        return 'prooph.event_streams';
    }

    protected function projectionsTable(): string
    {
        return 'prooph.event_projections';
    }

    /**
     * @test
     */
    public function it_updates_state_using_when_and_persists_with_block_size(): void
    {
        $this->prepareEventStream('prooph.user-123');

        $testCase = $this;

        $query = $this->projectionManager->createQuery();

        $query
            ->fromAll()
            ->when([
                UserCreated::class => function ($state, Message $event) use ($testCase): array {
                    $testCase->assertEquals('prooph.user-123', $this->streamName());
                    $state['name'] = $event->payload()['name'];

                    return $state;
                },
                UsernameChanged::class => function ($state, Message $event) use ($testCase): array {
                    $testCase->assertEquals('prooph.user-123', $this->streamName());
                    $state['name'] = $event->payload()['name'];

                    if ($event->payload()['name'] === 'Sascha') {
                        $this->stop();
                    }

                    return $state;
                },
            ])
            ->run();

        $this->assertEquals('Sascha', $query->getState()['name']);
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

        new PdoEventStoreQuery(
            $wrappedEventStore->reveal(),
            $this->connection,
            $this->eventStreamsTable()
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

        new PdoEventStoreQuery($eventStore->reveal(), $connection->reveal(), $this->eventStreamsTable());
    }
}
