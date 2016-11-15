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

namespace Prooph\EventStore\PDO\Projection;

use PDO;
use Prooph\EventStore\PDO\PostgresEventStore;
use Prooph\EventStore\Projection\AbstractProjection;

final class PostgresProjection extends AbstractProjection
{
    use PostgresQueryTrait;

    /**
     * @var string
     */
    protected $projectionsTable;

    public function __construct(
        PostgresEventStore $eventStore,
        PDO $connection,
        string $eventStreamsTable,
        string $projectionsTable,
        string $name,
        bool $enableEmit
    ) {
        parent::__construct($eventStore, $name, $enableEmit);

        $this->connection = $connection;
        $this->eventStreamsTable = $eventStreamsTable;
        $this->projectionsTable = $projectionsTable;
    }

    protected function load(): void
    {
        $sql = <<<EOT
SELECT position, state FROM $this->projectionsTable WHERE name = '$this->name' ORDER BY no DESC LIMIT 1;
EOT;
        $statement = $this->connection->prepare($sql);
        $statement->execute();

        $result = $statement->fetch(PDO::FETCH_OBJ);

        if (! $result) {
            return;
        }

        $this->position->merge(json_decode($result->position, true));
        $this->state = json_decode($result->state, true);
    }

    protected function persist(): void
    {
        $sql = <<<EOT
INSERT INTO $this->projectionsTable (name, position, state) VALUES (?, ?, ?); 
EOT;
        $statement = $this->connection->prepare($sql);
        $statement->execute([
            $this->name,
            json_encode($this->position->streamPositions()),
            json_encode($this->state)]
        );
    }

    protected function resetProjection(): void
    {
        $this->connection->beginTransaction();

        $deleteProjectionSql = <<<EOT
DELETE FROM $this->projectionsTable WHERE name = ?;
EOT;
        $statement = $this->connection->prepare($deleteProjectionSql);
        $statement->execute([$this->name]);

        if ($this->emitEnabled) {
            $deleteEventStreamTableEntrySql = <<<EOT
DELETE FROM $this->eventStreamsTable WHERE real_stream_name = ?;
EOT;
            $statement = $this->connection->prepare($deleteEventStreamTableEntrySql);
            $statement->execute([$this->name]);

            $deleteEventStreamSql = <<<EOT
DROP TABLE ?;
EOT;
            $statement = $this->connection->prepare($deleteEventStreamSql);
            $statement->execute('_' . sha1($this->name)); // @todo: make EventStore::deleteStream() method??
        }

        $this->connection->commit();
    }

    public function delete(bool $deleteEmittedEvents): void
    {
        $this->connection->beginTransaction();

        $deleteProjectionSql = <<<EOT
DELETE FROM $this->projectionsTable WHERE name = ?;
EOT;
        $statement = $this->connection->prepare($deleteProjectionSql);
        $statement->execute([$this->name]);

        if ($deleteEmittedEvents && $this->emitEnabled) {
            $deleteEventStreamTableEntrySql = <<<EOT
DELETE FROM $this->eventStreamsTable WHERE real_stream_name = ?;
EOT;
            $statement = $this->connection->prepare($deleteEventStreamTableEntrySql);
            $statement->execute([$this->name]);

            $deleteEventStreamSql = <<<EOT
DROP TABLE ?;
EOT;
            $statement = $this->connection->prepare($deleteEventStreamSql);
            $statement->execute('_' . sha1($this->name)); // @todo: make EventStore::deleteStream() method??
        }

        $this->connection->commit();
    }
}
