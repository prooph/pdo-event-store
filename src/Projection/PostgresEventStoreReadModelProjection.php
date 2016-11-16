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
use Prooph\EventStore\Projection\ReadModelProjection;

final class PostgresEventStoreReadModelProjection extends AbstractPDOReadModelProjection
{
    use PDOQueryTrait;

    /**
     * @var string
     */
    protected $projectionsTable;

    public function __construct(
        PostgresEventStore $eventStore,
        PDO $connection,
        string $name,
        ReadModelProjection $readModelProjection,
        string $eventStreamsTable,
        string $projectionsTable
    ) {
        parent::__construct(
            $eventStore,
            $connection,
            $name,
            $readModelProjection,
            $eventStreamsTable,
            $projectionsTable
        );
    }

    protected function persist(): void
    {
        $sql = <<<EOT
INSERT INTO $this->projectionsTable (name, position, state) 
VALUES (?, ?, ?) 
ON CONFLICT (name) 
DO UPDATE SET position = ?, state = ?; 
EOT;

        $jsonEncodedPosition = json_encode($this->position->streamPositions());
        $jsonEncodedState = json_encode($this->state);

        $statement = $this->connection->prepare($sql);
        $statement->execute([
            $this->name,
            $jsonEncodedPosition,
            $jsonEncodedState,
            $jsonEncodedPosition,
            $jsonEncodedState,
        ]);
    }
}
