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
use Prooph\EventStore\CanControlTransaction;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\Projection\AbstractProjection;
use Prooph\EventStore\StreamName;

abstract class AbstractPDOProjection extends AbstractProjection
{
    use PDOQueryTrait;

    /**
     * @var string
     */
    protected $eventStreamsTable;

    /**
     * @var string
     */
    protected $projectionsTable;

    public function __construct(
        EventStore $eventStore,
        PDO $connection,
        string $name,
        string $eventStreamsTable,
        string $projectionsTable,
        bool $emitEnabled
    ) {
        parent::__construct($eventStore, $name, $emitEnabled);

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

    protected function resetProjection(): void
    {
        if ($this->eventStore instanceof CanControlTransaction) {
            $this->eventStore->beginTransaction();
        }

        $deleteProjectionSql = <<<EOT
DELETE FROM $this->projectionsTable WHERE name = ?;
EOT;
        $statement = $this->connection->prepare($deleteProjectionSql);
        $statement->execute([$this->name]);

        $this->eventStore->delete(new StreamName($this->name));

        if ($this->eventStore instanceof CanControlTransaction) {
            $this->eventStore->commit();
        }
    }
}
