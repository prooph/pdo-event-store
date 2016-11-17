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
use Prooph\Common\Messaging\Message;
use Prooph\EventStore\Exception\RuntimeException;
use Prooph\EventStore\PDO\Projection\MySQLEventStoreReadModelProjection;
use Prooph\EventStore\StreamName;
use ProophTest\EventStore\Mock\ReadModelProjectionMock;
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\Mock\UsernameChanged;

/**
 * @group pdo_mysql
 */
class MySQLEventStoreReadModelProjectionTest extends AbstractMySQLEventStoreProjectionTest
{
    /**
     * @test
     */
    public function it_updates_read_model_using_when_and_loads_and_continues_again(): void
    {
        $this->prepareEventStream('user-123');

        $readModel = new ReadModelProjectionMock();

        $projection = new MySQLEventStoreReadModelProjection(
            $this->eventStore,
            $this->connection,
            'test_projection',
            $readModel,
            'event_streams',
            'projections',
            1000
        );

        $projection
            ->fromAll()
            ->when([
                UserCreated::class => function ($state, Message $event): void {
                    $this->readModelProjection()->insert('name', $event->payload()['name']);
                },
                UsernameChanged::class => function ($state, Message $event): void {
                    $this->readModelProjection()->update('name', $event->payload()['name']);
                }
            ])
            ->run();

        $this->assertEquals('Sascha', $readModel->read('name'));

        $projection = new MySQLEventStoreReadModelProjection(
            $this->eventStore,
            $this->connection,
            'test_projection',
            $readModel,
            'event_streams',
            'projections',
            1000
        );

        $projection
            ->fromAll()
            ->when([
                UserCreated::class => function ($state, Message $event): void {
                    $this->readModelProjection()->insert('name', $event->payload()['name']);
                },
                UsernameChanged::class => function ($state, Message $event): void {
                    $this->readModelProjection()->update('name', $event->payload()['name']);
                }
            ])
            ->run();

        $this->assertEquals('Sascha', $readModel->read('name'));

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

        $projection = new MySQLEventStoreReadModelProjection(
            $this->eventStore,
            $this->connection,
            'test_projection',
            $readModel,
            'event_streams',
            'projections',
            1000
        );

        $projection
            ->fromAll()
            ->when([
                UserCreated::class => function ($state, Message $event): void {
                    $this->readModelProjection()->insert('name', $event->payload()['name']);
                },
                UsernameChanged::class => function ($state, Message $event): void {
                    $this->readModelProjection()->update('name', $event->payload()['name']);
                }
            ])
            ->run();

        $this->assertEquals('Oliver', $readModel->read('name'));
    }

    /**
     * @test
     */
    public function it_updates_read_model_using_when_any(): void
    {
        $this->prepareEventStream('user-123');

        $readModel = new ReadModelProjectionMock();

        $projection = new MySQLEventStoreReadModelProjection(
            $this->eventStore,
            $this->connection,
            'test_projection',
            $readModel,
            'event_streams',
            'projections',
            1000
        );

        $projection
            ->init(function (): void {
                $this->readModelProjection()->insert('name', null);
            })
            ->fromStream('user-123')
            ->whenAny(function ($state, Message $event): void {
                $this->readModelProjection()->update('name', $event->payload()['name']);
            }
            )
            ->run();

        $this->assertEquals('Sascha', $readModel->read('name'));
    }

    /**
     * @test
     */
    public function it_updates_projection_and_deletes(): void
    {
        $this->prepareEventStream('user-123');

        $readModel = new ReadModelProjectionMock();

        $projection = new MySQLEventStoreReadModelProjection(
            $this->eventStore,
            $this->connection,
            'test_projection',
            $readModel,
            'event_streams',
            'projections',
            1000
        );

        $projection
            ->fromStream('user-123')
            ->when([
                UserCreated::class => function (array $state, UserCreated $event): array {
                    $this->readModelProjection()->insert('name', $event->payload()['name']);
                    return $state;
                }
            ])
            ->run();

        $this->assertEquals('Alex', $readModel->read('name'));

        $projection->delete(true);

        $this->assertFalse($readModel->hasKey('name'));
    }

    /**
     * @test
     */
    public function it_throws_exception_on_run_when_nothing_configured(): void
    {
        $this->expectException(RuntimeException::class);

        $readModel = new ReadModelProjectionMock();

        $projection = new MySQLEventStoreReadModelProjection(
            $this->eventStore,
            $this->connection,
            'test_projection',
            $readModel,
            'event_streams',
            'projections',
            1000
        );
        $projection->run();
    }
}
