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
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\Pdo\MariaDbEventStore;
use Prooph\EventStore\Pdo\PersistenceStrategy\MariaDbSimpleStreamStrategy;
use Prooph\EventStore\Pdo\Projection\MariaDbProjectionManager;
use ProophTest\EventStore\Pdo\TestUtil;

/**
 * @group mariadb
 */
class MariaDbEventStoreQueryCustomTablesTestCase extends PdoEventStoreQueryCustomTablesTestCase
{
    protected function setUp(): void
    {
        if (TestUtil::getDatabaseDriver() !== 'pdo_mysql') {
            throw new \RuntimeException('Invalid database driver');
        }

        $this->connection = TestUtil::getConnection();
        TestUtil::initCustomDatabaseTables($this->connection);

        $this->eventStore = new MariaDbEventStore(
            new FQCNMessageFactory(),
            $this->connection,
            new MariaDbSimpleStreamStrategy(new NoOpMessageConverter()),
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
}
