<?php

/**
 * This file is part of prooph/pdo-event-store.
 * (c) 2016-2022 Alexander Miertsch <kontakt@codeliner.ws>
 * (c) 2016-2022 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
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
use Prooph\EventStore\Pdo\MySqlEventStore;
use Prooph\EventStore\Pdo\PersistenceStrategy\MySqlSimpleStreamStrategy;
use Prooph\EventStore\Pdo\Projection\MySqlProjectionManager;
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\Pdo\TestUtil;

/**
 * @group mysql
 */
class MySqlEventStoreProjectorTest extends PdoEventStoreProjectorTest
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

    protected function setUpEventStoreWithControlledConnection(PDO $connection): EventStore
    {
        return new MySqlEventStore(
            new FQCNMessageFactory(),
            $connection,
            new MySqlSimpleStreamStrategy(new NoOpMessageConverter()),
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
        $this->expectExceptionMessage(\sprintf("Error 42S02. Maybe the projection table is not setup?\nError-Info: Table '%s.projections' doesn't exist", \getenv('DB_NAME')));

        $this->prepareEventStream('user-123');

        $this->connection->exec('DROP TABLE projections;');

        $projection = $this->projectionManager->createProjection('test_projection');

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

        $command = 'exec php ' . \realpath(__DIR__) . '/mysql-isolated-long-running-projection.php';
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
        $projection = $this->projectionManager->createProjection('test_projection');

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
