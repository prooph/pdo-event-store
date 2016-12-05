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

use Prooph\EventStore\PDO\Exception\RuntimeException;

trait PDOEventStoreProjectionTrait
{
    protected function createProjection(): void
    {
        $sql = <<<EOT
INSERT INTO $this->projectionsTable (name, position, state, locked_until)
VALUES (?, '{}', '{}', NULL);
EOT;
        $statement = $this->connection->prepare($sql);
        // we ignore any occuring error here (duplicate projection)
        $statement->execute([$this->name]);
    }

    /**
     * @throws RuntimeException
     */
    protected function acquireLock(): void
    {
        $now = new \DateTimeImmutable('now', new \DateTimeZone('UTC'));
        $nowString = $now->format('Y-m-d\TH:i:s.u');
        $lockUntilString = $now->modify('+' . (string) $this->lockTimeoutMs . ' ms')->format('Y-m-d\TH:i:s.u');

        $sql = <<<EOT
UPDATE $this->projectionsTable SET locked_until = ? WHERE name = ? AND (locked_until IS NULL OR locked_until < ?);
EOT;
        $statement = $this->connection->prepare($sql);
        $statement->execute([$lockUntilString, $this->name, $nowString]);

        if ($statement->rowCount() !== 1) {
            throw new RuntimeException('Another projection process is already running');
        }
    }

    protected function releaseLock(): void
    {
        $sql = <<<EOT
UPDATE $this->projectionsTable SET locked_until = NULL WHERE name = ?;
EOT;
        $statement = $this->connection->prepare($sql);
        $statement->execute([$this->name]);
    }

    protected function persist(bool $force): void
    {
        if ($force || $this->eventCounter === $this->persistBlockSize) {
            $now = new \DateTimeImmutable('now', new \DateTimeZone('UTC'));
            $lockUntilString = $now->modify('+' . (string) $this->lockTimeoutMs . ' ms')->format('Y-m-d\TH:i:s.u');

            $sql = <<<EOT
UPDATE $this->projectionsTable SET position = ?, state = ?, locked_until = ? 
WHERE name = ? 
EOT;
            $statement = $this->connection->prepare($sql);
            $statement->execute([
                json_encode($this->position->streamPositions()),
                json_encode($this->state),
                $lockUntilString,
                $this->name,
            ]);
        }
    }
}
