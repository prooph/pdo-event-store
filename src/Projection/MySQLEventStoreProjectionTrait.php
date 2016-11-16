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
use Prooph\EventStore\PDO\Exception\RuntimeException;

trait MySQLEventStoreProjectionTrait
{
    /**
     * @throws RuntimeException
     */
    protected function acquireLock(): void
    {
        $statement = $this->connection->prepare("SELECT GET_LOCK('$this->name', 1) as 'lock';");
        $statement->execute();

        $row = $statement->fetch(PDO::FETCH_OBJ);

        if ($row->lock !== '1') {
            throw new RuntimeException('Another projection process is already running');
        }
    }

    protected function releaseLock(): void
    {
        $statement = $this->connection->prepare("SELECT RELEASE_LOCK('$this->name');");
        $statement->execute();
    }

    protected function persist(): void
    {
        $sql = <<<EOT
INSERT INTO $this->projectionsTable (name, position, state) 
VALUES (?, ?, ?) 
ON DUPLICATE KEY 
UPDATE position = ?, state = ?; 
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
