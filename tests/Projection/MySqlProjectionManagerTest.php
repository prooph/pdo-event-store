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
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\Test;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\EventStoreDecorator;
use Prooph\EventStore\Pdo\Exception\InvalidArgumentException;
use Prooph\EventStore\Pdo\Exception\RuntimeException;
use Prooph\EventStore\Pdo\MySqlEventStore;
use Prooph\EventStore\Pdo\PersistenceStrategy\MySqlPersistenceStrategy;
use Prooph\EventStore\Pdo\Projection\MySqlProjectionManager;
use ProophTest\EventStore\Pdo\TestUtil;
use ProophTest\EventStore\Projection\AbstractProjectionManagerTestCase;
use Prophecy\PhpUnit\ProphecyTrait;

#[Group('mysql')]
class MySqlProjectionManagerTest extends AbstractProjectionManagerTestCase
{
    use ProphecyTrait;

    private MySqlEventStore $eventStore;

    private PDO $connection;

    protected function setUp(): void
    {
        if (TestUtil::getDatabaseDriver() !== 'pdo_mysql') {
            throw new \RuntimeException('Invalid database driver');
        }

        $this->connection = TestUtil::getConnection();
        TestUtil::initDefaultDatabaseTables($this->connection);

        $persistenceStrategy = $this->prophesize(MySqlPersistenceStrategy::class)->reveal();

        $this->eventStore = new MySqlEventStore(
            new FQCNMessageFactory(),
            $this->connection,
            $persistenceStrategy
        );
        $this->projectionManager = new MySqlProjectionManager($this->eventStore, $this->connection);
    }

    protected function tearDown(): void
    {
        TestUtil::tearDownDatabase();
    }

    #[Test]
    public function it_fetches_projection_names(): void
    {
        // Overwrite parent test for different test duration
        parent::it_fetches_projection_names();
    }

    #[Test]
    public function it_fetches_projection_names_using_regex(): void
    {
        // Overwrite parent test for different test duration
        parent::it_fetches_projection_names_using_regex();
    }

    #[Test]
    public function it_throws_exception_when_invalid_event_store_instance_passed(): void
    {
        $this->expectException(\Prooph\EventStore\Exception\InvalidArgumentException::class);

        $eventStore = $this->prophesize(EventStore::class);

        new MySqlProjectionManager($eventStore->reveal(), $this->connection);
    }

    #[Test]
    public function it_throws_exception_when_invalid_wrapped_event_store_instance_passed(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $eventStore = $this->prophesize(EventStore::class);
        $wrappedEventStore = $this->prophesize(EventStoreDecorator::class);
        $wrappedEventStore->getInnerEventStore()->willReturn($eventStore->reveal())->shouldBeCalled();

        new MySqlProjectionManager($wrappedEventStore->reveal(), $this->connection);
    }

    #[Test]
    public function it_throws_exception_when_fetching_projecton_names_with_missing_db_table(): void
    {
        $this->expectException(RuntimeException::class);

        $this->connection->exec('DROP TABLE projections;');
        $this->projectionManager->fetchProjectionNames(null, 200, 0);
    }

    #[Test]
    public function it_throws_exception_when_fetching_projecton_names_regex_with_missing_db_table(): void
    {
        $this->expectException(RuntimeException::class);

        $this->connection->exec('DROP TABLE projections;');
        $this->projectionManager->fetchProjectionNamesRegex('^foo', 200, 0);
    }
}
