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
use Prooph\EventStore\Projection\ReadModel;
use Prooph\EventStore\Util\ArrayCache;

final class PostgresEventStoreReadModelProjection extends AbstractPDOReadModelProjection
{
    /**
     * @var PDO
     */
    protected $connection;

    /**
     * @var string
     */
    protected $eventStreamsTable;

    /**
     * @var PostgresEventStore
     */
    protected $eventStore;

    /**
     * @var string
     */
    protected $projectionsTable;

    /**
     * @var EventStore
     */
    protected $eventStore;

    /**
     * @var array
     */
    private $streamPositions;

    /**
     * @var array
     */
    protected $state = [];

    /**
     * @var callable|null
     */
    protected $initCallback;

    /**
     * @var Closure|null
     */
    protected $handler;

    /**
     * @var array
     */
    protected $handlers = [];

    /**
     * @var boolean
     */
    protected $isStopped = false;

    /**
     * @var ?string
     */
    protected $currentStreamName = null;

    public function __construct(
        PostgresEventStore $eventStore,
        PDO $connection,
        string $name,
        ReadModel $readModel,
        string $eventStreamsTable,
        string $projectionsTable,
        int $lockTimeoutMs,
        int $cacheSize,
        int $persistBlockSize
    ) {
        $this->eventStore = $eventStore;
        $this->connection = $connection;
        $this->eventStreamsTable = $eventStreamsTable;
        $this->name = $name;
        $this->cachedStreamNames = new ArrayCache($cacheSize);
        $this->persistBlockSize = $persistBlockSize;
        $this->connection = $connection;
        $this->eventStreamsTable = $eventStreamsTable;
        $this->projectionsTable = $projectionsTable;
        $this->lockTimeoutMs = $lockTimeoutMs;
    }

    public function init(Closure $callback): Query
    {
        if (null !== $this->initCallback) {
            throw new RuntimeException('Projection already initialized');
        }

        $callback = Closure::bind($callback, $this->createHandlerContext($this->currentStreamName));

        $result = $callback();

        if (is_array($result)) {
            $this->state = $result;
        }

        $this->initCallback = $callback;

        return $this;
    }

    public function fromStream(string $streamName): Query
    {
        if (null !== $this->position) {
            throw new RuntimeException('From was already called');
        }

        $this->position = new Position([$streamName => 0]);

        return $this;
    }

    public function fromStreams(string ...$streamNames): Query
    {
        if (null !== $this->position) {
            throw new RuntimeException('From was already called');
        }

        $streamPositions = [];

        foreach ($streamNames as $streamName) {
            $streamPositions[$streamName] = 0;
        }

        $this->position = new Position($streamPositions);

        return $this;
    }

    public function fromCategory(string $name): Query
    {
        $sql = <<<EOT
SELECT real_stream_name FROM $this->eventStreamsTable WHERE real_stream_name LIKE '$name-%';
EOT;
        $statement = $this->connection->prepare($sql);
        $statement->execute();

        $streams = [];
        while ($row = $statement->fetch(PDO::FETCH_OBJ)) {
            $streams[$row->real_stream_name] = 0;
        }

        $this->position = new Position($streams);

        return $this;
    }

    public function fromCategories(string ...$names): Query
    {
        $it = new CachingIterator(new ArrayIterator($names), CachingIterator::FULL_CACHE);

        $where = 'WHERE ';
        foreach ($it as $name) {
            $where .= "real_stream_name LIKE '$name-%'";
            if ($it->hasNext()) {
                $where .= ' OR ';
            }
        }

        $sql = <<<EOT
SELECT real_stream_name FROM $this->eventStreamsTable $where;
EOT;
        $statement = $this->connection->prepare($sql);
        $statement->execute();

        $streams = [];
        while ($row = $statement->fetch(PDO::FETCH_OBJ)) {
            $streams[$row->real_stream_name] = 0;
        }

        $this->position = new Position($streams);

        return $this;
    }

    public function fromAll(): Query
    {
        $sql = <<<EOT
SELECT real_stream_name FROM $this->eventStreamsTable WHERE real_stream_name NOT LIKE '$%';
EOT;
        $statement = $this->connection->prepare($sql);
        $statement->execute();

        $streams = [];
        while ($row = $statement->fetch(PDO::FETCH_OBJ)) {
            $streams[$row->real_stream_name] = 0;
        }

        $this->position = new Position($streams);

        return $this;
    }

    public function when(array $handlers): Query
    {
        if (null !== $this->handler || ! empty($this->handlers)) {
            throw new RuntimeException('When was already called');
        }

        foreach ($handlers as $eventName => $handler) {
            if (! is_string($eventName)) {
                throw new InvalidArgumentException('Invalid event name given, string expected');
            }

            if (! $handler instanceof Closure) {
                throw new InvalidArgumentException('Invalid handler given, Closure expected');
            }

            $this->handlers[$eventName] = Closure::bind($handler, $this->createHandlerContext($this->currentStreamName));
        }

        return $this;
    }

    public function whenAny(Closure $handler): Query
    {
        if (null !== $this->handler || ! empty($this->handlers)) {
            throw new RuntimeException('When was already called');
        }

        $this->handler = Closure::bind($handler, $this->createHandlerContext($this->currentStreamName));

        return $this;
    }

    public function reset(): void
    {
        if (null !== $this->position) {
            $this->position->reset();
        }

        $callback = $this->initCallback;

        if (is_callable($callback)) {
            $result = $callback();

            if (is_array($result)) {
                $this->state = $result;

                return;
            }
        }

        $this->state = [];
    }

    public function run(): void
    {
        if (null === $this->position
            || (null === $this->handler && empty($this->handlers))
        ) {
            throw new RuntimeException('No handlers configured');
        }

        $singleHandler = null !== $this->handler;

        foreach ($this->position->streamPositions() as $streamName => $position) {
            $stream = $this->eventStore->load(new StreamName($streamName), $position + 1);

            if ($singleHandler) {
                $this->handleStreamWithSingleHandler($streamName, $stream->streamEvents());
            } else {
                $this->handleStreamWithHandlers($streamName, $stream->streamEvents());
            }

            if ($this->isStopped) {
                break;
            }
        }
    }

    public function stop(): void
    {
        $this->isStopped = true;
    }

    public function getState(): array
    {
        return $this->state;
    }

    private function handleStreamWithSingleHandler(string $streamName, Iterator $events): void
    {
        $this->currentStreamName = $streamName;
        $handler = $this->handler;

        foreach ($events as $event) {
            /* @var Message $event */
            $this->position->inc($streamName);

            $result = $handler($this->state, $event);

            if (is_array($result)) {
                $this->state = $result;
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
            $this->position->inc($streamName);

            if (! isset($this->handlers[$event->messageName()])) {
                continue;
            }

            $handler = $this->handlers[$event->messageName()];
            $result = $handler($this->state, $event);

            if (is_array($result)) {
                $this->state = $result;
            }

            if ($this->isStopped) {
                break;
            }
        }
    }

    protected function createHandlerContext(?string &$streamName)
    {
        return new class($this, $streamName) {
            /**
             * @var Query
             */
            private $query;

            /**
             * @var ?string
             */
            private $streamName;

            public function __construct(Query $query, ?string &$streamName)
            {
                $this->query = $query;
                $this->streamName = &$streamName;
            }

            public function stop(): void
            {
                $this->query->stop();
            }

            public function streamName(): ?string
            {
                return $this->streamName;
            }
        };
    }

    public function delete(bool $deleteEmittedEvents): void
    {
        $this->eventStore->beginTransaction();

        $deleteProjectionSql = <<<EOT
DELETE FROM $this->projectionsTable WHERE name = ?;
EOT;
        $statement = $this->connection->prepare($deleteProjectionSql);
        $statement->execute([$this->name]);

        $this->eventStore->commit();

        parent::delete($deleteEmittedEvents);
    }

    abstract protected function createProjection(): void;

    /**
     * @throws RuntimeException
     */
    abstract protected function acquireLock(): void;

    abstract protected function releaseLock(): void;

    public function run(bool $keepRunning = true): void
    {
        $this->createProjection();
        $this->acquireLock();

        try {
            parent::run($keepRunning);
        } finally {
            $this->releaseLock();
        }
    }

    protected function load(): void
    {
        $sql = <<<EOT
SELECT position, state FROM $this->projectionsTable WHERE name = '$this->name' ORDER BY no DESC LIMIT 1;
EOT;
        $statement = $this->connection->prepare($sql);
        $statement->execute();

        $result = $statement->fetch(PDO::FETCH_OBJ);

        $this->position->merge(json_decode($result->position, true));
        $state = json_decode($result->state, true);

        if (! empty($state)) {
            $this->state = $state;
        }
    }

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
            if ($statement->errorCode() !== '00000') {
                $errorCode = $statement->errorCode();
                $errorInfo = $statement->errorInfo()[2];

                throw new RuntimeException(
                    "Error $errorCode. Maybe the projection table is not setup?\nError-Info: $errorInfo"
                );
            }

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

    protected function persist(): void
    {
        if ($this instanceof ReadModelProjection) {
            $this->readModel()->persist();
        }

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
