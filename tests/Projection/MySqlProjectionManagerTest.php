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

use PDO;
use PHPUnit\Framework\TestCase;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\EventStoreDecorator;
use Prooph\EventStore\Pdo\Exception\InvalidArgumentException;
use Prooph\EventStore\Pdo\Exception\RuntimeException;
use Prooph\EventStore\Pdo\MySqlEventStore;
use Prooph\EventStore\Pdo\PersistenceStrategy\MySqlAggregateStreamStrategy;
use Prooph\EventStore\Pdo\Projection\MySqlProjectionManager;
use Prooph\EventStore\Projection\InMemoryProjectionManager;
use Prooph\EventStore\Projection\ProjectionStatus;
use ProophTest\EventStore\Mock\ReadModelMock;
use ProophTest\EventStore\Pdo\TestUtil;

/**
 * @group pdo_mysql
 */
class MySqlProjectionManagerTest extends TestCase
{
    /**
     * @var MySqlProjectionManager
     */
    private $projectionManager;

    /**
     * @var MySqlEventStore
     */
    private $eventStore;

    /**
     * @var PDO
     */
    private $connection;

    protected function setUp(): void
    {
        if (TestUtil::getDatabaseVendor() !== 'pdo_mysql') {
            throw new \RuntimeException('Invalid database vendor');
        }

        $this->connection = TestUtil::getConnection();
        TestUtil::initDefaultDatabaseTables($this->connection);

        $this->eventStore = $this->createEventStore($this->connection);
        $this->projectionManager = new MySqlProjectionManager($this->eventStore, $this->connection);
    }

    protected function tearDown(): void
    {
        $this->connection->exec('DROP TABLE event_streams;');
        $this->connection->exec('DROP TABLE projections;');
    }

    protected function createEventStore(PDO $connection): MySqlEventStore
    {
        return new MySqlEventStore(
            new FQCNMessageFactory(),
            $connection,
            new MySqlAggregateStreamStrategy()
        );
    }

    /**
     * @test
     */
    public function it_fetches_projection_names(): void
    {
        $projections = [];

        try {
            for ($i = 0; $i < 50; $i++) {
                $projection = $this->projectionManager->createProjection('user-' . $i);
                $projection->fromAll()->whenAny(function (): void {
                })->run(false);
                $projections[] = $projection;
            }

            for ($i = 0; $i < 20; $i++) {
                $projection = $this->projectionManager->createProjection(uniqid('rand'));
                $projection->fromAll()->whenAny(function (): void {
                })->run(false);
                $projections[] = $projection;
            }

            $this->assertCount(1, $this->projectionManager->fetchProjectionNames('user-0', 200, 0));
            $this->assertCount(70, $this->projectionManager->fetchProjectionNames(null, 200, 0));
            $this->assertCount(0, $this->projectionManager->fetchProjectionNames(null, 200, 100));
            $this->assertCount(10, $this->projectionManager->fetchProjectionNames(null, 10, 0));
            $this->assertCount(10, $this->projectionManager->fetchProjectionNames(null, 10, 10));
            $this->assertCount(5, $this->projectionManager->fetchProjectionNames(null, 10, 65));

            for ($i = 0; $i < 20; $i++) {
                $this->assertStringStartsWith('rand', $this->projectionManager->fetchProjectionNames(null, 1, $i)[0]);
            }

            $this->assertCount(30, $this->projectionManager->fetchProjectionNamesRegex('ser-', 30, 0));
            $this->assertCount(0, $this->projectionManager->fetchProjectionNamesRegex('n-', 30, 0));
        } finally {
            foreach ($projections as $projection) {
                $projection->delete(false);
            }
        }
    }

    /**
     * @test
     */
    public function it_throws_exception_when_fetching_projecton_names_with_missing_db_table(): void
    {
        $this->expectException(RuntimeException::class);

        $this->connection->exec('DROP TABLE projections;');
        $this->projectionManager->fetchProjectionNames(null, 200, 0);
    }

    /**
     * @test
     */
    public function it_throws_exception_when_fetching_projection_names_using_invalid_regex(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid regex pattern given');

        $this->projectionManager->fetchProjectionNamesRegex('invalid)', 10, 0);
    }

    /**
     * @test
     */
    public function it_throws_exception_when_invalid_event_store_instance_passed(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $eventStore = $this->prophesize(EventStore::class);

        new InMemoryProjectionManager($eventStore->reveal());
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

        new InMemoryProjectionManager($wrappedEventStore->reveal());
    }

    /**
     * @test
     */
    public function it_cannot_delete_projections(): void
    {
        $this->expectException(RuntimeException::class);

        $this->projectionManager->deleteProjection('foo', true);
    }

    /**
     * @test
     */
    public function it_cannot_reset_projections(): void
    {
        $this->expectException(RuntimeException::class);

        $this->projectionManager->resetProjection('foo');
    }

    /**
     * @test
     */
    public function it_cannot_stop_projections(): void
    {
        $this->expectException(RuntimeException::class);

        $this->projectionManager->stopProjection('foo');
    }

    /**
     * @test
     */
    public function it_throws_exception_when_asked_for_unknown_projection_status(): void
    {
        $this->expectException(RuntimeException::class);

        $this->projectionManager->fetchProjectionStatus('unkown');
    }

    /**
     * @test
     */
    public function it_throws_exception_when_asked_for_unknown_projection_stream_positions(): void
    {
        $this->expectException(RuntimeException::class);

        $this->projectionManager->fetchProjectionStreamPositions('unkown');
    }

    /**
     * @test
     */
    public function it_throws_exception_when_asked_for_unknown_projection_state(): void
    {
        $this->expectException(RuntimeException::class);

        $this->projectionManager->fetchProjectionState('unkown');
    }

    /**
     * @test
     */
    public function it_fetches_projection_status(): void
    {
        $this->projectionManager->createProjection('test-projection');

        $this->assertSame(ProjectionStatus::IDLE(), $this->projectionManager->fetchProjectionStatus('test-projection'));
    }

    /**
     * @test
     */
    public function it_fetches_projection_stream_positions(): void
    {
        $this->projectionManager->createProjection('test-projection');

        $this->assertSame(null, $this->projectionManager->fetchProjectionStreamPositions('test-projection'));
    }

    /**
     * @test
     */
    public function it_fetches_projection_state(): void
    {
        $this->projectionManager->createProjection('test-projection');

        $this->assertSame([], $this->projectionManager->fetchProjectionState('test-projection'));
    }
}
