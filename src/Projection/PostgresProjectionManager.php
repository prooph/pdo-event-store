<?php
/**
 * This file is part of the prooph/pdo-event-store.
 * (c) 2016-2017 prooph software GmbH <contact@prooph.de>
 * (c) 2016-2017 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Prooph\EventStore\Pdo\Projection;

use PDO;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\EventStoreDecorator;
use Prooph\EventStore\Pdo\Exception;
use Prooph\EventStore\Pdo\PostgresEventStore;
use Prooph\EventStore\Projection\Projection;
use Prooph\EventStore\Projection\ProjectionManager;
use Prooph\EventStore\Projection\ProjectionStatus;
use Prooph\EventStore\Projection\Query;
use Prooph\EventStore\Projection\ReadModel;
use Prooph\EventStore\Projection\ReadModelProjection;

final class PostgresProjectionManager implements ProjectionManager
{
    public const OPTION_CACHE_SIZE = 'cache_size';
    public const OPTION_SLEEP = 'sleep';
    public const OPTION_PERSIST_BLOCK_SIZE = 'persist_block_size';
    public const OPTION_LOCK_TIMEOUT_MS = 'lock_timeout_ms';

    private const DEFAULT_CACHE_SIZE = 1000;
    private const DEFAULT_SLEEP = 100000;
    private const DEFAULT_PERSIST_BLOCK_SIZE = 1000;
    private const DEFAULT_LOCK_TIMEOUT_MS = 1000;

    /**
     * @var EventStore
     */
    private $eventStore;

    /**
     * @var PDO
     */
    private $connection;

    /**
     * @var string
     */
    private $eventStreamsTable;

    /**
     * @var string
     */
    private $projectionsTable;

    public function __construct(
        EventStore $eventStore,
        PDO $connection,
        string $eventStreamsTable,
        string $projectionsTable
    ) {
        $this->eventStore = $eventStore;
        $this->connection = $connection;
        $this->eventStreamsTable = $eventStreamsTable;
        $this->projectionsTable = $projectionsTable;

        while ($eventStore instanceof EventStoreDecorator) {
            $eventStore = $eventStore->getInnerEventStore();
        }

        if (! $eventStore instanceof PostgresEventStore) {
            throw new Exception\InvalidArgumentException('Unknown event store instance given');
        }
    }

    public function createQuery(): Query
    {
        return new PdoEventStoreQuery($this->eventStore, $this->connection, $this->eventStreamsTable);
    }

    public function createProjection(
        string $name,
        array $options = []
    ): Projection {
        return new PdoEventStoreProjection(
            $this->eventStore,
            $this->connection,
            $name,
            $this->eventStreamsTable,
            $this->projectionsTable,
            $options[self::DEFAULT_LOCK_TIMEOUT_MS] ?? self::DEFAULT_LOCK_TIMEOUT_MS,
            $options[self::OPTION_CACHE_SIZE] ?? self::DEFAULT_CACHE_SIZE,
            $options[self::DEFAULT_PERSIST_BLOCK_SIZE] ?? self::DEFAULT_PERSIST_BLOCK_SIZE,
            $options[self::OPTION_SLEEP] ?? self::DEFAULT_SLEEP
        );
    }

    public function createReadModelProjection(
        string $name,
        ReadModel $readModel,
        array $options = []
    ): ReadModelProjection {
        return new PdoEventStoreReadModelProjection(
            $this->eventStore,
            $this->connection,
            $name,
            $readModel,
            $this->eventStreamsTable,
            $this->projectionsTable,
            $options[self::DEFAULT_LOCK_TIMEOUT_MS] ?? self::DEFAULT_LOCK_TIMEOUT_MS,
            $options[self::DEFAULT_PERSIST_BLOCK_SIZE] ?? self::DEFAULT_PERSIST_BLOCK_SIZE,
            $options[self::OPTION_SLEEP] ?? self::DEFAULT_SLEEP
        );
    }

    public function deleteProjection(string $name, bool $deleteEmittedEvents): void
    {
        $sql = <<<EOT
UPDATE $this->projectionsTable SET status = ? WHERE name = ?;
EOT;

        if ($deleteEmittedEvents) {
            $status = ProjectionStatus::DELETING_INCL_EMITTED_EVENTS()->getValue();
        } else {
            $status = ProjectionStatus::DELETING()->getValue();
        }

        $statement = $this->connection->prepare($sql);
        $statement->execute([
            $status,
            $name,
        ]);
    }

    public function resetProjection(string $name): void
    {
        $sql = <<<EOT
UPDATE $this->projectionsTable SET status = ? WHERE name = ?;
EOT;

        $statement = $this->connection->prepare($sql);
        $statement->execute([
            ProjectionStatus::RESETTING()->getValue(),
            $name,
        ]);
    }

    public function stopProjection(string $name): void
    {
        $sql = <<<EOT
UPDATE $this->projectionsTable SET status = ? WHERE name = ?;
EOT;

        $statement = $this->connection->prepare($sql);
        $statement->execute([
            ProjectionStatus::STOPPING()->getValue(),
            $name,
        ]);
    }

    public function fetchProjectionNames(?string $filter, int $limit = 20, int $offset = 0): array
    {
        $where = [];
        $values = [];

        if (null !== $filter) {
            $where[] = 'name = :filter ';
            $values[':filter'] = $filter;
        }

        $whereCondition = implode(' AND ', $where);

        if (! empty($whereCondition)) {
            $whereCondition = 'WHERE ' . $whereCondition;
        }

        $query = <<<SQL
SELECT name FROM $this->projectionsTable
$whereCondition
ORDER BY name ASC
LIMIT $limit OFFSET $offset
SQL;

        $statement = $this->connection->prepare($query);
        $statement->setFetchMode(PDO::FETCH_OBJ);
        $statement->execute($values);

        if ($statement->errorCode() !== '00000') {
            $errorCode = $statement->errorCode();
            $errorInfo = $statement->errorInfo()[2];

            throw new Exception\RuntimeException(
                "Error $errorCode. Maybe the event streams table is not setup?\nError-Info: $errorInfo"
            );
        }

        $result = $statement->fetchAll();

        $projectionNames = [];

        foreach ($result as $projectionName) {
            $projectionNames[] = $projectionName->name;
        }

        return $projectionNames;
    }

    public function fetchProjectionNamesRegex(string $filter, int $limit = 20, int $offset = 0): array
    {
        if (false === @preg_match("/$filter/", '')) {
            throw new Exception\InvalidArgumentException('Invalid regex pattern given');
        }

        $where = [];
        $values = [];

        $where[] = 'name ~ :filter ';
        $values[':filter'] = $filter;

        $whereCondition = 'WHERE ' . implode(' AND ', $where);

        $query = <<<SQL
SELECT name FROM $this->projectionsTable
$whereCondition
ORDER BY name ASC
LIMIT $limit OFFSET $offset
SQL;

        $statement = $this->connection->prepare($query);
        $statement->setFetchMode(PDO::FETCH_OBJ);
        $statement->execute($values);

        if ($statement->errorCode() !== '00000') {
            $errorCode = $statement->errorCode();
            $errorInfo = $statement->errorInfo()[2];

            throw new Exception\RuntimeException(
                "Error $errorCode. Maybe the event streams table is not setup?\nError-Info: $errorInfo"
            );
        }

        $result = $statement->fetchAll();

        $projectionNames = [];

        foreach ($result as $projectionName) {
            $projectionNames[] = $projectionName->name;
        }

        return $projectionNames;
    }

    public function fetchProjectionStatus(string $name): ProjectionStatus
    {
        // TODO: Implement fetchProjectionStatus() method.
    }

    public function fetchProjectionStreamPositions(string $name): ?array
    {
        // TODO: Implement fetchProjectionStreamPositions() method.
    }

    public function fetchProjectionState(string $name): array
    {
        // TODO: Implement fetchProjectionState() method.
    }
}
