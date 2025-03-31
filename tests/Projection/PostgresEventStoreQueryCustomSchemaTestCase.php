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
use ProophTest\EventStore\Pdo\TestUtil;

/**
 * @group postgres
 */
class PostgresEventStoreQueryCustomSchemaTestCase extends PdoEventStoreQueryCustomSchemaTestCase
{
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
}
