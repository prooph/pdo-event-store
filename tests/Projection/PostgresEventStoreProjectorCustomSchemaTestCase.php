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

use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\EventStore\Pdo\PersistenceStrategy\PostgresSimpleStreamStrategy;
use Prooph\EventStore\Pdo\PostgresEventStore;
use Prooph\EventStore\Pdo\Projection\PostgresProjectionManager;
use Prooph\EventStore\Pdo\Util\PostgresHelper;
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\Pdo\TestUtil;

/**
 * @group postgres
 */
class PostgresEventStoreProjectorCustomSchemaTestCase extends PdoEventStoreProjectorCustomSchemaTestCase
{
    use PostgresHelper;

    protected function setUp(): void
    {
        if (TestUtil::getDatabaseDriver() !== 'pdo_pgsql') {
            throw new \RuntimeException('Invalid database vendor');
        }

        $this->connection = TestUtil::getConnection();
        TestUtil::initCustomSchemaDatabaseTables($this->connection);

        $this->eventStore = new PostgresEventStore(
            new FQCNMessageFactory(),
            TestUtil::getConnection(),
            new PostgresSimpleStreamStrategy(),
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

    /**
     * @test
     */
    public function it_handles_missing_projection_table(): void
    {
        $this->expectException(\Prooph\EventStore\Pdo\Exception\RuntimeException::class);
        $this->expectExceptionMessage("Error 42P01. Maybe the projection table is not setup?\nError-Info: ERROR:  relation \"{$this->projectionsTable()}\" does not exist\nLINE 1: SELECT status FROM");

        $this->prepareEventStream('prooph.user-123');

        $this->connection->exec("DROP TABLE {$this->quoteIdent($this->projectionsTable())};");

        $projection = $this->projectionManager->createProjection('test_projection');

        $projection
            ->fromStream('prooph.user-123')
            ->when([
                UserCreated::class => function (array $state, UserCreated $event): array {
                    $this->stop();

                    return $state;
                },
            ])
            ->run();
    }
}
