<?php
/**
 * This file is part of the prooph/pdo-event-store.
 * (c) 2016-2018 prooph software GmbH <contact@prooph.de>
 * (c) 2016-2018 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
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
use Prooph\EventStore\Pdo\PersistenceStrategy\MariaDbAggregateStreamStrategy;
use Prooph\EventStore\Pdo\Projection\MariaDbProjectionManager;
use ProophTest\EventStore\Pdo\TestUtil;
use ProophTest\EventStore\Projection\AbstractProjectionManagerTest;

/**
 * @group mariadb
 */
class MariaDbProjectionManagerTest extends AbstractProjectionManagerTest
{
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
        TestUtil::initDefaultDatabaseTables($this->connection);

        $this->eventStore = new MariaDbEventStore(
            new FQCNMessageFactory(),
            $this->connection,
            new MariaDbAggregateStreamStrategy()
        );
        $this->projectionManager = new MariaDbProjectionManager($this->eventStore, $this->connection);
    }

    protected function tearDown(): void
    {
        $this->connection->exec('DROP TABLE IF EXISTS event_streams;');
        $this->connection->exec('DROP TABLE IF EXISTS projections;');
    }

    /**
     * @test
     */
    public function it_throws_exception_when_invalid_event_store_instance_passed(): void
    {
        $this->expectException(\Prooph\EventStore\Exception\InvalidArgumentException::class);

        $eventStore = $this->prophesize(EventStore::class);

        new MariaDbProjectionManager($eventStore->reveal(), $this->connection);
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

        new MariaDbProjectionManager($wrappedEventStore->reveal(), $this->connection);
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
    public function it_throws_exception_when_fetching_projecton_names_regex_with_missing_db_table(): void
    {
        $this->expectException(RuntimeException::class);

        $this->connection->exec('DROP TABLE projections;');
        $this->projectionManager->fetchProjectionNamesRegex('^foo', 200, 0);
    }
}
