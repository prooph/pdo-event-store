<?php
/**
 * This file is part of the prooph/pdo-event-store.
 * (c) 2016-2016 prooph software GmbH <contact@prooph.de>
 * (c) 2016-2016 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace ProophTest\EventStore\PDO\Projection;

use ArrayIterator;
use PDO;
use PHPUnit\Framework\TestCase;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\Mock\UsernameChanged;

abstract class ProjectionTestCase extends TestCase
{
    /**
     * @var EventStore
     */
    protected $eventStore;

    /**
     * @var PDO
     */
    protected $connection;

    protected function tearDown(): void
    {
        // these tables are used in every test case
        $this->connection->exec('DROP TABLE event_streams;');
        $this->connection->exec('DROP TABLE projections;');
        $this->connection->exec('DROP TABLE _' . sha1('user-123'));
        // these tables are used only in some test cases
        $this->connection->exec('DROP TABLE IF EXISTS _' . sha1('user-234'));
        $this->connection->exec('DROP TABLE IF EXISTS _' . sha1('$iternal-345'));
        $this->connection->exec('DROP TABLE IF EXISTS _' . sha1('guest-345'));
        $this->connection->exec('DROP TABLE IF EXISTS _' . sha1('guest-456'));
        $this->connection->exec('DROP TABLE IF EXISTS _' . sha1('foo'));
        $this->connection->exec('DROP TABLE IF EXISTS _' . sha1('test_projection'));
    }

    protected function prepareEventStream(string $name): void
    {
        $events = [];
        $events[] = UserCreated::with([
            'name' => 'Alex',
        ], 1);
        for ($i = 2; $i < 50; $i++) {
            $events[] = UsernameChanged::with([
                'name' => uniqid('name_'),
            ], $i);
        }
        $events[] = UsernameChanged::with([
            'name' => 'Sascha',
        ], 50);

        $this->eventStore->create(new Stream(new StreamName($name), new ArrayIterator($events)));
    }
}
