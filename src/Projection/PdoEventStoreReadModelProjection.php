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

use ArrayIterator;
use CachingIterator;
use Closure;
use DateTimeImmutable;
use DateTimeZone;
use Iterator;
use PDO;
use Prooph\Common\Messaging\Message;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\EventStoreDecorator;
use Prooph\EventStore\Exception;
use Prooph\EventStore\Pdo\MySqlEventStore;
use Prooph\EventStore\Pdo\PostgresEventStore;
use Prooph\EventStore\Projection\Projection;
use Prooph\EventStore\Projection\ReadModel;
use Prooph\EventStore\Projection\ReadModelProjection;
use Prooph\EventStore\StreamName;

final class PdoEventStoreReadModelProjection implements ReadModelProjection
{
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
    private $name;

    /**
     * @var ReadModel
     */
    private $readModel;

    /**
     * @var string
     */
    private $eventStreamsTable;

    /**
     * @var string
     */
    private $projectionsTable;

    /**
     * @var array
     */
    private $streamPositions;

    /**
     * @var int
     */
    private $persistBlockSize;

    /**
     * @var array
     */
    private $state = [];

    /**
     * @var callable|null
     */
    private $initCallback;

    /**
     * @var Closure|null
     */
    private $handler;

    /**
     * @var array
     */
    private $handlers = [];

    /**
     * @var boolean
     */
    private $isStopped = false;

    /**
     * @var ?string
     */
    private $currentStreamName = null;

    /**
     * @var int lock timeout in milliseconds
     */
    private $lockTimeoutMs;

    /**
     * @var int
     */
    private $eventCounter = 0;

    /**
     * @var int
     */
    private $sleep;

    public function __construct(
        EventStore $eventStore,
        PDO $connection,
        string $name,
        ReadModel $readModel,
        string $eventStreamsTable,
        string $projectionsTable,
        int $lockTimeoutMs,
        int $persistBlockSize,
        int $sleep
    ) {
        $this->eventStore = $eventStore;
        $this->connection = $connection;
        $this->name = $name;
        $this->readModel = $readModel;
        $this->eventStreamsTable = $eventStreamsTable;
        $this->projectionsTable = $projectionsTable;
        $this->lockTimeoutMs = $lockTimeoutMs;
        $this->persistBlockSize = $persistBlockSize;
        $this->sleep = $sleep;

        while ($eventStore instanceof EventStoreDecorator) {
            $eventStore = $eventStore->getInnerEventStore();
        }

        if (! $eventStore instanceof MySqlEventStore
            && ! $eventStore instanceof PostgresEventStore
        ) {
            throw new Exception\InvalidArgumentException('Unknown event store instance given');
        }
    }

    public function init(Closure $callback): ReadModelProjection
    {
        if (null !== $this->initCallback) {
            throw new Exception\RuntimeException('Projection already initialized');
        }

        $callback = Closure::bind($callback, $this->createHandlerContext($this->currentStreamName));

        $result = $callback();

        if (is_array($result)) {
            $this->state = $result;
        }

        $this->initCallback = $callback;

        return $this;
    }

    public function fromStream(string $streamName): ReadModelProjection
    {
        if (null !== $this->streamPositions) {
            throw new Exception\RuntimeException('From was already called');
        }

        $this->streamPositions = [$streamName => 0];

        return $this;
    }

    public function fromStreams(string ...$streamNames): ReadModelProjection
    {
        if (null !== $this->streamPositions) {
            throw new Exception\RuntimeException('From was already called');
        }

        foreach ($streamNames as $streamName) {
            $this->streamPositions[$streamName] = 0;
        }

        return $this;
    }

    public function fromCategory(string $name): ReadModelProjection
    {
        if (null !== $this->streamPositions) {
            throw new Exception\RuntimeException('From was already called');
        }

        $sql = <<<EOT
SELECT real_stream_name FROM $this->eventStreamsTable WHERE real_stream_name LIKE ?;
EOT;
        $statement = $this->connection->prepare($sql);
        $statement->execute([$name . '-%']);

        $this->streamPositions = [];

        while ($row = $statement->fetch(PDO::FETCH_OBJ)) {
            $this->streamPositions[$row->real_stream_name] = 0;
        }

        return $this;
    }

    public function fromCategories(string ...$names): ReadModelProjection
    {
        if (null !== $this->streamPositions) {
            throw new Exception\RuntimeException('From was already called');
        }

        $it = new CachingIterator(new ArrayIterator($names), CachingIterator::FULL_CACHE);

        $where = 'WHERE ';
        $params = [];
        foreach ($it as $name) {
            $where .= 'real_stream_name LIKE ?';
            if ($it->hasNext()) {
                $where .= ' OR ';
            }
            $params[] = $name . '-%';
        }

        $sql = <<<EOT
SELECT real_stream_name FROM $this->eventStreamsTable $where;
EOT;
        $statement = $this->connection->prepare($sql);
        $statement->execute($params);

        $this->streamPositions = [];

        while ($row = $statement->fetch(PDO::FETCH_OBJ)) {
            $this->streamPositions[$row->real_stream_name] = 0;
        }

        return $this;
    }

    public function fromAll(): ReadModelProjection
    {
        if (null !== $this->streamPositions) {
            throw new Exception\RuntimeException('From was already called');
        }

        $sql = <<<EOT
SELECT real_stream_name FROM $this->eventStreamsTable WHERE real_stream_name NOT LIKE '$%';
EOT;
        $statement = $this->connection->prepare($sql);
        $statement->execute();

        $this->streamPositions = [];

        while ($row = $statement->fetch(PDO::FETCH_OBJ)) {
            $this->streamPositions[$row->real_stream_name] = 0;
        }

        return $this;
    }

    public function when(array $handlers): ReadModelProjection
    {
        if (null !== $this->handler || ! empty($this->handlers)) {
            throw new Exception\RuntimeException('When was already called');
        }

        foreach ($handlers as $eventName => $handler) {
            if (! is_string($eventName)) {
                throw new Exception\InvalidArgumentException('Invalid event name given, string expected');
            }

            if (! $handler instanceof Closure) {
                throw new Exception\InvalidArgumentException('Invalid handler given, Closure expected');
            }

            $this->handlers[$eventName] = Closure::bind($handler, $this->createHandlerContext($this->currentStreamName));
        }

        return $this;
    }

    public function whenAny(Closure $handler): ReadModelProjection
    {
        if (null !== $this->handler || ! empty($this->handlers)) {
            throw new Exception\RuntimeException('When was already called');
        }

        $this->handler = Closure::bind($handler, $this->createHandlerContext($this->currentStreamName));

        return $this;
    }

    public function reset(): void
    {
        if (null !== $this->streamPositions) {
            $this->streamPositions = array_map(
                function (): int {
                    return 0;
                },
                $this->streamPositions
            );
        }

        $callback = $this->initCallback;

        $this->resetProjection();

        if (is_callable($callback)) {
            $result = $callback();

            if (is_array($result)) {
                $this->state = $result;

                return;
            }
        }

        $this->state = [];
    }

    public function stop(): void
    {
        $this->isStopped = true;
    }

    public function getState(): array
    {
        return $this->state;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function readModel(): ReadModel
    {
        return $this->readModel;
    }

    public function delete(bool $deleteProjection): void
    {
        $deleteProjectionSql = <<<EOT
DELETE FROM $this->projectionsTable WHERE name = ?;
EOT;
        $statement = $this->connection->prepare($deleteProjectionSql);
        $statement->execute([$this->name]);

        if ($deleteProjection) {
            $this->readModel->delete();
        }
    }

    public function run(bool $keepRunning = true): void
    {
        if (null === $this->streamPositions
            || (null === $this->handler && empty($this->handlers))
        ) {
            throw new Exception\RuntimeException('No handlers configured');
        }

        $this->createProjection();
        $this->acquireLock();

        if (! $this->readModel->isInitialized()) {
            $this->readModel->init();
        }

        try {
            do {
                $this->load();

                $singleHandler = null !== $this->handler;

                foreach ($this->streamPositions as $streamName => $position) {
                    try {
                        $streamEvents = $this->eventStore->load(new StreamName($streamName), $position + 1);
                    } catch (Exception\StreamNotFound $e) {
                        // no newer events found
                        continue;
                    }

                    if ($singleHandler) {
                        $this->handleStreamWithSingleHandler($streamName, $streamEvents);
                    } else {
                        $this->handleStreamWithHandlers($streamName, $streamEvents);
                    }

                    if ($this->isStopped) {
                        break;
                    }
                }

                if (0 === $this->eventCounter) {
                    usleep($this->sleep);
                } else {
                    $this->persist();
                }

                $this->eventCounter = 0;
            } while ($keepRunning && ! $this->isStopped);
        } finally {
            $this->releaseLock();
        }
    }

    private function handleStreamWithSingleHandler(string $streamName, Iterator $events): void
    {
        $this->currentStreamName = $streamName;
        $handler = $this->handler;

        foreach ($events as $event) {
            /* @var Message $event */
            $this->streamPositions[$streamName]++;
            $this->eventCounter++;

            $result = $handler($this->state, $event);

            if (is_array($result)) {
                $this->state = $result;
            }

            if ($this->eventCounter === $this->persistBlockSize) {
                $this->persist();
                $this->eventCounter = 0;
            }

            if ($this->isStopped) {
                break;
            }
        }
    }

    private function handleStreamWithHandlers(string $streamName, Iterator $events): void
    {
        $this->currentStreamName = $streamName;
        foreach ($events as $event) {
            /* @var Message $event */
            $this->streamPositions[$streamName]++;
            $this->eventCounter++;

            if (! isset($this->handlers[$event->messageName()])) {
                continue;
            }

            $handler = $this->handlers[$event->messageName()];
            $result = $handler($this->state, $event);

            if (is_array($result)) {
                $this->state = $result;
            }

            if ($this->eventCounter === $this->persistBlockSize) {
                $this->persist();
                $this->eventCounter = 0;
            }

            if ($this->isStopped) {
                break;
            }
        }
    }

    private function createHandlerContext(?string &$streamName)
    {
        return new class($this, $streamName) {
            /**
             * @var ReadModelProjection
             */
            private $projection;

            /**
             * @var ?string
             */
            private $streamName;

            public function __construct(ReadModelProjection $projection, ?string &$streamName)
            {
                $this->projection = $projection;
                $this->streamName = &$streamName;
            }

            public function stop(): void
            {
                $this->projection->stop();
            }

            public function readModel(): ReadModel
            {
                return $this->projection->readModel();
            }

            public function streamName(): ?string
            {
                return $this->streamName;
            }
        };
    }

    private function load(): void
    {
        $sql = <<<EOT
SELECT position, state FROM $this->projectionsTable WHERE name = '$this->name' ORDER BY no DESC LIMIT 1;
EOT;
        $statement = $this->connection->prepare($sql);
        $statement->execute();

        $result = $statement->fetch(PDO::FETCH_OBJ);

        $this->streamPositions = array_merge($this->streamPositions, json_decode($result->position, true));
        $state = json_decode($result->state, true);

        if (! empty($state)) {
            $this->state = $state;
        }
    }

    private function resetProjection(): void
    {
        $deleteProjectionSql = <<<EOT
DELETE FROM $this->projectionsTable WHERE name = ?;
EOT;
        $statement = $this->connection->prepare($deleteProjectionSql);
        $statement->execute([$this->name]);

        $this->readModel->reset();
    }

    private function createProjection(): void
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
     * @throws Exception\RuntimeException
     */
    private function acquireLock(): void
    {
        $now = new DateTimeImmutable('now', new DateTimeZone('UTC'));
        $nowString = $now->format('Y-m-d\TH:i:s.u');
        $lockUntilString = $now->modify('+' . (string) $this->lockTimeoutMs . ' ms')->format('Y-m-d\TH:i:s.u');

        $sql = <<<EOT
UPDATE $this->projectionsTable SET locked_until = ? WHERE name = ? AND (locked_until IS NULL OR locked_until < ?);
EOT;
        $statement = $this->connection->prepare($sql);
        $statement->execute([$lockUntilString, $this->name, $nowString]);

        if ($statement->rowCount() !== 1) {
            if ($statement->errorCode() !== '00000') {
                $errorCode = $statement->errorCode();
                $errorInfo = $statement->errorInfo()[2];

                throw new Exception\RuntimeException(
                    "Error $errorCode. Maybe the projection table is not setup?\nError-Info: $errorInfo"
                );
            }

            throw new Exception\RuntimeException('Another projection process is already running');
        }
    }

    private function releaseLock(): void
    {
        $sql = <<<EOT
UPDATE $this->projectionsTable SET locked_until = NULL WHERE name = ?;
EOT;

        $statement = $this->connection->prepare($sql);
        $statement->execute([$this->name]);
    }

    private function persist(): void
    {
        $this->readModel->persist();

        $now = new DateTimeImmutable('now', new DateTimeZone('UTC'));
        $lockUntilString = $now->modify('+' . (string) $this->lockTimeoutMs . ' ms')->format('Y-m-d\TH:i:s.u');

        $sql = <<<EOT
UPDATE $this->projectionsTable SET position = ?, state = ?, locked_until = ? 
WHERE name = ? 
EOT;
        $statement = $this->connection->prepare($sql);
        $statement->execute([
            json_encode($this->streamPositions),
            json_encode($this->state),
            $lockUntilString,
            $this->name,
        ]);
    }
}
