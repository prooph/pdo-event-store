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
use Prooph\EventStore\Pdo\PersistenceStrategy\PostgresSimpleStreamStrategy;
use Prooph\EventStore\Pdo\PostgresEventStore;
use Prooph\EventStore\Pdo\Projection\PostgresProjectionManager;
use ProophTest\EventStore\Mock\ReadModelMock;
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\Pdo\TestUtil;

/**
 * @group postgres
 */
class PostgresEventStoreReadModelProjectorTestCase extends PdoEventStoreReadModelProjectorTestCase
{
    protected function setUp(): void
    {
        if (TestUtil::getDatabaseDriver() !== 'pdo_pgsql') {
            throw new \RuntimeException('Invalid database vendor');
        }

        $this->connection = TestUtil::getConnection();
        TestUtil::initDefaultDatabaseTables($this->connection);

        $this->eventStore = new PostgresEventStore(
            new FQCNMessageFactory(),
            TestUtil::getConnection(),
            new PostgresSimpleStreamStrategy(new NoOpMessageConverter())
        );

        $this->projectionManager = new PostgresProjectionManager(
            $this->eventStore,
            $this->connection
        );
    }

    protected function setUpEventStoreWithControlledConnection(PDO $connection): EventStore
    {
        return new PostgresEventStore(
            new FQCNMessageFactory(),
            $connection,
            new PostgresSimpleStreamStrategy(new NoOpMessageConverter()),
            10000,
            'event_streams',
            true
        );
    }

    /**
     * @test
     */
    public function it_handles_missing_projection_table(): void
    {
        $this->expectException(\Prooph\EventStore\Pdo\Exception\RuntimeException::class);
        $this->expectExceptionMessage("Error 42P01. Maybe the projection table is not setup?\nError-Info: ERROR:  relation \"projections\" does not exist\nLINE 1: SELECT status FROM");

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

        $command = 'exec php ' . \realpath(__DIR__) . '/postgres-isolated-long-running-read-model-projection.php';
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
INSERT INTO "projections" (name, position, state, status, locked_until)
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
SELECT * FROM "projections" WHERE name = ?
EOT;

        $statement = $this->connection->prepare($sql);
        $statement->execute([
            'test_projection',
        ]);

        $row = $statement->fetch(\PDO::FETCH_ASSOC);

        $this->assertEquals(['user' => 10], \json_decode($row['position'], true));
    }
}
