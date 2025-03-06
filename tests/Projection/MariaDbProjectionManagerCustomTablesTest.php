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

use PDO;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\EventStoreDecorator;
use Prooph\EventStore\Pdo\Exception\InvalidArgumentException;
use Prooph\EventStore\Pdo\Exception\RuntimeException;
use Prooph\EventStore\Pdo\MariaDbEventStore;
use Prooph\EventStore\Pdo\PersistenceStrategy\MariaDbPersistenceStrategy;
use Prooph\EventStore\Pdo\Projection\MariaDbProjectionManager;
use ProophTest\EventStore\Pdo\TestUtil;
use ProophTest\EventStore\Projection\AbstractProjectionManagerTest;
use Prophecy\PhpUnit\ProphecyTrait;

/**
 * @group mariadb
 */
class MariaDbProjectionManagerCustomTablesTest extends AbstractProjectionManagerTest
{
    use ProphecyTrait;

    /**
     * @var MariaDbProjectionManager
     */
    protected $projectionManager;

    /**
     * @var MariaDbEventStore
     */
    private $eventStore;

    /**
     * @var PDO
     */
    private $connection;

    protected function setUp(): void
    {
        if (TestUtil::getDatabaseDriver() !== 'pdo_mysql') {
            throw new \RuntimeException('Invalid database driver');
        }

        $this->connection = TestUtil::getConnection();
        TestUtil::initCustomDatabaseTables($this->connection);

        $persistenceStrategy = $this->prophesize(MariaDbPersistenceStrategy::class)->reveal();

        $this->eventStore = new MariaDbEventStore(
            new FQCNMessageFactory(),
            $this->connection,
            $persistenceStrategy,
            10000,
            'events/streams'
        );
        $this->projectionManager = new MariaDbProjectionManager(
            $this->eventStore,
            $this->connection,
            'events/streams',
            'events/projections'
        );
    }

    protected function tearDown(): void
    {
        TestUtil::tearDownDatabase();
    }

    /**
     * @test
     */
    public function it_throws_exception_when_invalid_event_store_instance_passed(): void
    {
        $this->expectException(\Prooph\EventStore\Exception\InvalidArgumentException::class);

        $eventStore = $this->prophesize(EventStore::class);

        new MariaDbProjectionManager($eventStore->reveal(), $this->connection, 'events/streams', 'events/projections');
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

        new MariaDbProjectionManager($wrappedEventStore->reveal(), $this->connection, 'events/streams', 'events/projections');
    }

    /**
     * @test
     */
    public function it_throws_exception_when_fetching_projecton_names_with_missing_db_table(): void
    {
        $this->expectException(RuntimeException::class);

        $this->connection->exec('DROP TABLE `events/projections`;');
        $this->projectionManager->fetchProjectionNames(null, 200, 0);
    }

    /**
     * @test
     */
    public function it_throws_exception_when_fetching_projecton_names_regex_with_missing_db_table(): void
    {
        $this->expectException(RuntimeException::class);

        $this->connection->exec('DROP TABLE `events/projections`;');
        $this->projectionManager->fetchProjectionNamesRegex('^foo', 200, 0);
    }
}
