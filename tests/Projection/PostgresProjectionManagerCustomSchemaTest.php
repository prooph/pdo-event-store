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
use Prooph\EventStore\Pdo\PersistenceStrategy\PostgresAggregateStreamStrategy;
use Prooph\EventStore\Pdo\PostgresEventStore;
use Prooph\EventStore\Pdo\Projection\PostgresProjectionManager;
use Prooph\EventStore\Pdo\Util\PostgresHelper;
use Prooph\EventStore\Projection\InMemoryProjectionManager;
use ProophTest\EventStore\Pdo\TestUtil;
use ProophTest\EventStore\Projection\AbstractProjectionManagerTestCase;
use Prophecy\PhpUnit\ProphecyTrait;

#[Group('postgres')]
class PostgresProjectionManagerCustomSchemaTest extends AbstractProjectionManagerTestCase
{
    use PostgresHelper;
    use ProphecyTrait;

    private PostgresEventStore $eventStore;

    private PDO $connection;

    protected function setUp(): void
    {
        if (TestUtil::getDatabaseDriver() !== 'pdo_pgsql') {
            throw new \RuntimeException('Invalid database vendor');
        }

        $this->connection = TestUtil::getConnection();
        TestUtil::initCustomSchemaDatabaseTables($this->connection);

        $this->eventStore = new PostgresEventStore(
            new FQCNMessageFactory(),
            $this->connection,
            new PostgresAggregateStreamStrategy(),
            10000,
            $this->eventStreamsTable()
        );
        $this->projectionManager = new PostgresProjectionManager(
            $this->eventStore,
            $this->connection,
            $this->eventStreamsTable(),
            $this->projectionsTable()
        );
    }

    protected function eventStreamsTable(): string
    {
        return 'prooph.event_streams';
    }

    protected function projectionsTable(): string
    {
        return 'prooph.event_projections';
    }

    protected function tearDown(): void
    {
        TestUtil::tearDownDatabase();
    }

    #[Test]
    public function it_throws_exception_when_invalid_event_store_instance_passed(): void
    {
        $this->expectException(\Prooph\EventStore\Exception\InvalidArgumentException::class);

        $eventStore = $this->prophesize(EventStore::class);

        new InMemoryProjectionManager($eventStore->reveal());
    }

    #[Test]
    public function it_throws_exception_when_invalid_wrapped_event_store_instance_passed(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $eventStore = $this->prophesize(EventStore::class);
        $wrappedEventStore = $this->prophesize(EventStoreDecorator::class);
        $wrappedEventStore->getInnerEventStore()->willReturn($eventStore->reveal())->shouldBeCalled();

        new PostgresProjectionManager($wrappedEventStore->reveal(), $this->connection);
    }

    #[Test]
    public function it_throws_exception_when_fetching_projecton_names_with_missing_db_table(): void
    {
        $this->expectException(RuntimeException::class);

        $this->connection->exec("DROP TABLE {$this->quoteIdent($this->projectionsTable())};");
        $this->projectionManager->fetchProjectionNames(null, 200, 0);
    }

    #[Test]
    public function it_throws_exception_when_fetching_projection_names_using_invalid_regex(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid regex pattern given');

        $this->projectionManager->fetchProjectionNamesRegex('invalid)', 10, 0);
    }

    #[Test]
    public function it_throws_exception_when_fetching_projecton_names_regex_with_missing_db_table(): void
    {
        $this->expectException(RuntimeException::class);

        $this->connection->exec("DROP TABLE {$this->quoteIdent($this->projectionsTable())};");
        $this->projectionManager->fetchProjectionNamesRegex('^foo', 200, 0);
    }
}
