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
use Prooph\EventStore\Pdo\MySqlEventStore;
use Prooph\EventStore\Pdo\PersistenceStrategy\MySqlSimpleStreamStrategy;
use Prooph\EventStore\Pdo\Projection\MySqlProjectionManager;
use ProophTest\EventStore\Pdo\TestUtil;

/**
 * @group mysql
 */
class MySqlEventStoreQueryTestCase extends PdoEventStoreQueryTestCase
{
    protected function setUp(): void
    {
        if (TestUtil::getDatabaseDriver() !== 'pdo_mysql') {
            throw new \RuntimeException('Invalid database driver');
        }

        $this->isMariaDb = TestUtil::getDatabaseVendor() === 'mariadb';

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
     * @small
     */
    public function it_stops_immediately_after_pcntl_signal_was_received(): void
    {
        if (! \extension_loaded('pcntl')) {
            $this->markTestSkipped('The PCNTL extension is not available.');

            return;
        }

        $command = 'exec php ' . \realpath(__DIR__) . '/mysql-isolated-long-running-query.php';
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
}
