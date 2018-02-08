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

use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\EventStore\Pdo\MariaDbEventStore;
use Prooph\EventStore\Pdo\PersistenceStrategy\MariaDbSimpleStreamStrategy;
use Prooph\EventStore\Pdo\Projection\MariaDbProjectionManager;
use Prooph\EventStore\Projection\ReadModel;
use ProophTest\EventStore\Mock\ReadModelMock;
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\Pdo\TestUtil;

/**
 * @group mariadb
 */
class MariaDbEventStoreReadModelProjectorCustomTablesTest extends PdoEventStoreReadModelProjectorCustomTablesTest
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
            new MariaDbSimpleStreamStrategy(),
            10000,
            'estreams'

        );

        $this->projectionManager = new MariaDbProjectionManager(
            $this->eventStore,
            $this->connection,
            'estreams',
            'eprojections'
        );
    }

    /**
     * @test
     */
    public function it_calls_reset_projection_also_if_init_callback_returns_state()
    {
        $readModel = $this->prophesize(ReadModel::class);
        $readModel->reset()->shouldBeCalled();

        $readModelProjection = $this->projectionManager->createReadModelProjection('test-projection', $readModel->reveal());

        $readModelProjection->init(function () {
            return ['state' => 'some value'];
        });

        $readModelProjection->reset();
    }

    /**
     * @test
     */
    public function it_handles_missing_projection_table(): void
    {
        $this->expectException(\Prooph\EventStore\Pdo\Exception\RuntimeException::class);
        $this->expectExceptionMessage("Error 42S02. Maybe the projection table is not setup?\nError-Info: Table 'event_store_tests.eprojections' doesn't exist");

        $this->prepareEventStream('user-123');

        $this->connection->exec('DROP TABLE eprojections;');

        $projection = $this->projectionManager->createReadModelProjection('test_projection', new ReadModelMock());

        $projection
            ->fromStream('user-123')
            ->when([
                UserCreated::class => function (array $state, UserCreated $event): array {
                    $this->stop();

                    return $state;
                },
            ])
            ->run();
    }
}
