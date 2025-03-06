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
use Prooph\Common\Messaging\Message;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\Pdo\MariaDbEventStore;
use Prooph\EventStore\Pdo\PersistenceStrategy\MariaDbSimpleStreamStrategy;
use Prooph\EventStore\Pdo\Projection\MariaDbProjectionManager;
use Prooph\EventStore\Projection\ReadModel;
use ProophTest\EventStore\Mock\ReadModelMock;
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\Pdo\TestUtil;
use Prophecy\PhpUnit\ProphecyTrait;

/**
 * @group mariadb
 */
class MariaDbEventStoreReadModelProjectorTestCase extends PdoEventStoreReadModelProjectorTestCase
{
    use ProphecyTrait;

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
            new MariaDbSimpleStreamStrategy(new NoOpMessageConverter())
        );

        $this->projectionManager = new MariaDbProjectionManager(
            $this->eventStore,
            $this->connection
        );
    }

    protected function setUpEventStoreWithControlledConnection(PDO $connection): EventStore
    {
        return new MariaDbEventStore(
            new FQCNMessageFactory(),
            $connection,
            new MariaDbSimpleStreamStrategy(new NoOpMessageConverter()),
            10000,
            'event_streams',
            true
        );
    }

    /**
     * @test
     */
    public function it_calls_reset_projection_also_if_init_callback_returns_state(): void
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
        $this->expectExceptionMessage(\sprintf("Error 42S02. Maybe the projection table is not setup?\nError-Info: Table '%s.projections' doesn't exist", \getenv('DB_NAME')));

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

        $command = 'exec php ' . \realpath(__DIR__) . '/mariadb-isolated-long-running-read-model-projection.php';
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
}
