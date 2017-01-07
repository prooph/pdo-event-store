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

namespace Prooph\EventStore\PDO\Projection;

use ArrayIterator;
use CachingIterator;
use Closure;
use Iterator;
use PDO;
use Prooph\Common\Messaging\Message;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\EventStoreDecorator;
use Prooph\EventStore\Exception;
use Prooph\EventStore\PDO\MySQLEventStore;
use Prooph\EventStore\PDO\PostgresEventStore;
use Prooph\EventStore\Projection\Query;
use Prooph\EventStore\StreamName;

final class PDOEventStoreQuery implements Query
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
    private $eventStreamsTable;

    /**
     * @var array
     */
    private $streamPositions;

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

    public function __construct(EventStore $eventStore, PDO $connection, string $eventStreamsTable)
    {
        $this->eventStore = $eventStore;
        $this->connection = $connection;
        $this->eventStreamsTable = $eventStreamsTable;

        while ($eventStore instanceof EventStoreDecorator) {
            $eventStore = $eventStore->getInnerEventStore();
        }

        if (! $eventStore instanceof MySQLEventStore
            && ! $eventStore instanceof PostgresEventStore
        ) {
            throw new Exception\InvalidArgumentException('Unknown event store instance given');
        }
    }

    public function init(Closure $callback): Query
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

    public function fromStream(string $streamName): Query
    {
        if (null !== $this->streamPositions) {
            throw new Exception\RuntimeException('From was already called');
        }

        $this->streamPositions = [$streamName => 0];

        return $this;
    }

    public function fromStreams(string ...$streamNames): Query
    {
        if (null !== $this->streamPositions) {
            throw new Exception\RuntimeException('From was already called');
        }

        foreach ($streamNames as $streamName) {
            $this->streamPositions[$streamName] = 0;
        }

        return $this;
    }

    public function fromCategory(string $name): Query
    {
        if (null !== $this->streamPositions) {
            throw new Exception\RuntimeException('From was already called');
        }

        $sql = <<<EOT
SELECT real_stream_name FROM $this->eventStreamsTable WHERE real_stream_name LIKE '$name-%';
EOT;
        $statement = $this->connection->prepare($sql);
        $statement->execute();

        $this->streamPositions = [];

        while ($row = $statement->fetch(PDO::FETCH_OBJ)) {
            $this->streamPositions[$row->real_stream_name] = 0;
        }

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

        if (null !== $this->streamPositions) {
            throw new Exception\RuntimeException('From was already called');
        }

        $this->streamPositions = [];

        while ($row = $statement->fetch(PDO::FETCH_OBJ)) {
            $this->streamPositions[$row->real_stream_name] = 0;
        }

        return $this;
    }

    public function fromAll(): Query
    {
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

    public function when(array $handlers): Query
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

    public function whenAny(Closure $handler): Query
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

    public function run(): void
    {
        if (null === $this->streamPositions
            || (null === $this->handler && empty($this->handlers))
        ) {
            throw new Exception\RuntimeException('No handlers configured');
        }

        $singleHandler = null !== $this->handler;

        foreach ($this->streamPositions as $streamName => $position) {
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

    private function handleStreamWithSingleHandler(string $streamName, Iterator $events): void
    {
        $this->currentStreamName = $streamName;
        $handler = $this->handler;

        foreach ($events as $event) {
            /* @var Message $event */
            $this->streamPositions[$streamName]++;

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
            $this->streamPositions[$streamName]++;

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

    private function createHandlerContext(?string &$streamName)
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
}
