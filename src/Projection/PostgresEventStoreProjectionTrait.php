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

trait PostgresEventStoreProjectionTrait
{
    /**
     * @var \PDO
     */
    protected $connection;

    /**
     * @var string
     */
    private $uniqueId;

    private function uniqueId(): string
    {
        if (null === $this->uniqueId) {
            $this->uniqueId = (string) random_int(0, PHP_INT_MAX);
        }

        return $this->uniqueId;
    }

    /**
     * @throws RuntimeException
     */
    protected function acquireLock(): void
    {
        $sql = <<<EOT
SELECT locked FROM $this->projectionsTable WHERE name = '$this->name';
EOT;
        $statement = $this->connection->prepare($sql);
        $statement->execute();

        $row = $statement->fetch(PDO::FETCH_OBJ);

        if (false === $row) {
            $sql = <<<EOT
INSERT INTO $this->projectionsTable (name, position, state, locked) 
VALUES (?, ?, ?, ?);
EOT;
            $statement = $this->connection->prepare($sql);
            $statement->execute([
                $this->name,
                '{}',
                '{}',
                $this->uniqueId()
            ]);

            if ($statement->errorCode() !== '00000') {
                throw new RuntimeException('Another projection process is already running');
            }
            return;
        } elseif ($row->locked !== null) {
            throw new RuntimeException('Another projection process is already running');
        }
    }

    protected function releaseLock(): void
    {
        $statement = $this->connection->prepare("UPDATE $this->projectionsTable SET locked = NULL WHERE name = '$this->name';");
        $statement->execute();
    }

    protected function persist(): void
    {
        $sql = <<<EOT
UPDATE $this->projectionsTable SET position = ?, state = ?, locked = ? 
WHERE name = ? 
EOT;
        $statement = $this->connection->prepare($sql);
        $statement->execute([
            json_encode($this->position->streamPositions()),
            json_encode($this->state),
            $this->uniqueId(),
            $this->name,
        ]);
    }
}
