<?php

/**
 * This file is part of prooph/pdo-event-store.
 * (c) 2016-2018 prooph software GmbH <contact@prooph.de>
 * (c) 2016-2018 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace ProophTest\EventStore\Pdo\Projection;

use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\Message;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\Pdo\MySqlEventStore;
use Prooph\EventStore\Pdo\PersistenceStrategy\MySqlSimpleStreamStrategy;
use Prooph\EventStore\Pdo\Projection\MySqlProjectionManager;
use Prooph\EventStore\Projection\Projector;
use Prooph\EventStore\Projection\ReadModel;
use ProophTest\EventStore\Mock\ReadModelMock;
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\Mock\UsernameChanged;
use ProophTest\EventStore\Pdo\TestUtil;

/**
 * @group mysql
 */
class MySqlEventStoreReadModelProjectorTest extends PdoEventStoreReadModelProjectorTest
{
    protected function setUp(): void
    {
        if (TestUtil::getDatabaseDriver() !== 'pdo_mysql') {
            throw new \RuntimeException('Invalid database driver');
        }

        $this->connection = TestUtil::getConnection();
        TestUtil::initDefaultDatabaseTables($this->connection);

        $this->eventStore = new MySqlEventStore(
            new FQCNMessageFactory(),
            $this->connection,
            new MySqlSimpleStreamStrategy(new NoOpMessageConverter())
        );

        $this->projectionManager = new MySqlProjectionManager(
            $this->eventStore,
            $this->connection
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
        $this->expectExceptionMessage("Error 42S02. Maybe the projection table is not setup?\nError-Info: Table 'event_store_tests.projections' doesn't exist");

        $this->prepareEventStream('user-123');

        $this->connection->exec('DROP TABLE projections;');

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

    /**
     * @test
     * @small
     */
    public function it_stops_immediately_after_pcntl_signal_was_received(): void
    {
        if (! \extension_loaded('pcntl')) {
            $this->markTestSkipped('The PCNTL extension is not available.');

            return;
        }

        $command = 'exec php ' . \realpath(__DIR__) . '/mysql-isolated-long-running-read-model-projection.php';
        $descriptorSpec = [
            0 => ['pipe', 'r'],
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ];
        /**
         * Created process inherits env variables from this process.
         * Script returns with non-standard code SIGUSR1 from the handler and -1 else
         */
        $projectionProcess = \proc_open($command, $descriptorSpec, $pipes);
        $processDetails = \proc_get_status($projectionProcess);
        \usleep(500000);
        \posix_kill($processDetails['pid'], SIGQUIT);
        \usleep(500000);

        $processDetails = \proc_get_status($projectionProcess);
        $this->assertEquals(
            SIGUSR1,
            $processDetails['exitcode']
        );
    }

    /**
     * @test
     */
    public function a_stopped_status_should_keep_stream_positions(): void
    {
        $sql = <<<EOT
INSERT INTO `projections` (name, position, state, status, locked_until)
VALUES (?, ?, '{}', ?, NULL);
EOT;

        $statement = $this->connection->prepare($sql);
        $statement->execute([
            'test_projection',
            \json_encode(['user' => 10]),
            'stopping',
        ]);

        $this->prepareEventStream('user');
        $projection = $this->projectionManager->createReadModelProjection('test_projection', new ReadModelMock());

        $projection
            ->fromStream('user')
            ->init(function () {
                return ['count' => 0];
            })
            ->whenAny(
                function (array $state, Message $event): array {
                }
            )
            ->run();

        $sql = <<<EOT
SELECT * FROM `projections` WHERE name = ?
EOT;

        $statement = $this->connection->prepare($sql);
        $statement->execute([
            'test_projection',
        ]);

        $row = $statement->fetch(\PDO::FETCH_ASSOC);

        $this->assertEquals(['user' => 10], \json_decode($row['position'], true));
    }

    /**
     * @test
     * @testWith        [1, 70]
     *                  [20, 70]
     *                  [21, 71]
     */
    public function a_running_projector_that_is_reset_should_keep_stream_positions(int $blockSize, int $expectedEventsCount): void
    {
        $this->prepareEventStream('user-123');

        $projection = $this->projectionManager->createReadModelProjection('test_projection', new ReadModelMock(), [
            Projector::OPTION_PERSIST_BLOCK_SIZE => $blockSize,
        ]);

        $projectionManager = $this->projectionManager;

        $eventCounter = 0;

        $projection
            ->fromAll()
            ->when([
                       UserCreated::class => function ($state, Message $event) use (&$eventCounter): void {
                           $eventCounter++;
                       },
                       UsernameChanged::class => function ($state, Message $event) use (&$eventCounter, $projectionManager): void {
                           $eventCounter++;

                           if ($eventCounter === 20) {
                               $projectionManager->resetProjection('test_projection');
                           }

                           if ($eventCounter === 70) {
                               $projectionManager->stopProjection('test_projection');
                           }
                       },
                   ])
            ->run(true);

        $this->assertSame($expectedEventsCount, $eventCounter);
    }
}
