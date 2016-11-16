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
use Prooph\Common\Event\ActionEvent;
use Prooph\EventStore\ActionEventEmitterAware;
use Prooph\EventStore\PDO\MySQLEventStore;
use Prooph\EventStore\StreamName;

final class MySQLEventStoreProjection extends AbstractPDOProjection
{
    use PDOQueryTrait;

    /**
     * @var MySQLEventStore
     */
    protected $eventStore;

    public function __construct(
        MySQLEventStore $eventStore,
        PDO $connection,
        string $name,
        string $eventStreamsTable,
        string $projectionsTable,
        bool $emitEnabled
    ) {
        parent::__construct($eventStore, $connection, $name, $eventStreamsTable, $projectionsTable, $emitEnabled);
    }

    public function delete(bool $deleteEmittedEvents): void
    {
        $this->connection->beginTransaction();

        $listener = $this->eventStore->getActionEventEmitter()->attachListener(
            ActionEventEmitterAware::EVENT_APPEND_TO,
            function (ActionEvent $event): void {
                $event->setParam('isInTransaction', true);
            },
            1000
        );

        $deleteProjectionSql = <<<EOT
DELETE FROM $this->projectionsTable WHERE name = ?;
EOT;
        $statement = $this->connection->prepare($deleteProjectionSql);
        $statement->execute([$this->name]);

        if ($deleteEmittedEvents) {
            $this->eventStore->delete(new StreamName($this->name));
        }

        $this->connection->commit();

        $this->eventStore->getActionEventEmitter()->detachListener($listener);
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
