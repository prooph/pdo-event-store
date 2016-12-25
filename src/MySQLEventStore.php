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

namespace Prooph\EventStore\PDO;

use Iterator;
use PDO;
use Prooph\Common\Messaging\MessageConverter;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\Exception\ConcurrencyException;
use Prooph\EventStore\Exception\StreamExistsAlready;
use Prooph\EventStore\Exception\StreamNotFound;
use Prooph\EventStore\Metadata\MetadataMatcher;
use Prooph\EventStore\PDO\Exception\ExtensionNotLoaded;
use Prooph\EventStore\PDO\Exception\InvalidArgumentException;
use Prooph\EventStore\PDO\Exception\RuntimeException;
use Prooph\EventStore\PDO\Projection\MySQLEventStoreProjection;
use Prooph\EventStore\PDO\Projection\MySQLEventStoreQuery;
use Prooph\EventStore\PDO\Projection\MySQLEventStoreReadModelProjection;
use Prooph\EventStore\PDO\Projection\ProjectionOptions;
use Prooph\EventStore\Projection\Projection;
use Prooph\EventStore\Projection\ProjectionOptions as BaseProjectionOptions;
use Prooph\EventStore\Projection\Query;
use Prooph\EventStore\Projection\ReadModel;
use Prooph\EventStore\Projection\ReadModelProjection;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;

final class MySQLEventStore implements EventStore
{
    /**
     * @var MessageFactory
     */
    private $messageFactory;

    /**
     * @var MessageConverter
     */
    private $messageConverter;

    /**
     * @var PDO
     */
    private $connection;

    /**
     * @var PersistenceStrategy
     */
    private $persistenceStrategy;

    /**
     * @var int
     */
    private $loadBatchSize;

    /**
     * @var string
     */
    private $eventStreamsTable;

    /**
     * @var bool
     */
    private $duringCreate = false;

    /**
     * @throws ExtensionNotLoaded
     */
    public function __construct(
        MessageFactory $messageFactory,
        MessageConverter $messageConverter,
        PDO $connection,
        PersistenceStrategy $persistenceStrategy,
        int $loadBatchSize = 10000,
        string $eventStreamsTable = 'event_streams'
    ) {
        if (! extension_loaded('pdo_mysql')) {
            throw ExtensionNotLoaded::with('pdo_mysql');
        }

        $this->messageFactory = $messageFactory;
        $this->messageConverter = $messageConverter;
        $this->connection = $connection;
        $this->persistenceStrategy = $persistenceStrategy;
        $this->loadBatchSize = $loadBatchSize;
        $this->eventStreamsTable = $eventStreamsTable;
    }

    public function fetchStreamMetadata(StreamName $streamName): array
    {
        $sql = <<<EOT
SELECT metadata FROM $this->eventStreamsTable
WHERE real_stream_name = :streamName; 
EOT;

        $statement = $this->connection->prepare($sql);
        $statement->execute(['streamName' => $streamName->toString()]);

        $stream = $statement->fetch(PDO::FETCH_OBJ);

        if (! $stream) {
            throw StreamNotFound::with($streamName);
        }

        return json_decode($stream->metadata, true);
    }

    public function updateStreamMetadata(StreamName $streamName, array $newMetadata): void
    {
        $eventStreamsTable = $this->eventStreamsTable;

        $sql = <<<EOT
UPDATE $eventStreamsTable
SET metadata = :metadata
WHERE real_stream_name = :streamName; 
EOT;

        $statement = $this->connection->prepare($sql);
        $statement->execute([
            'streamName' => $streamName->toString(),
            'metadata' => json_encode($newMetadata),
        ]);

        if (1 !== $statement->rowCount()) {
            throw StreamNotFound::with($streamName);
        }
    }

    public function hasStream(StreamName $streamName): bool
    {
        $sql = <<<EOT
SELECT stream_name FROM $this->eventStreamsTable
WHERE real_stream_name = :streamName;
EOT;
        $statement = $this->connection->prepare($sql);

        $statement->execute(['streamName' => $streamName->toString()]);

        $stream = $statement->fetch(PDO::FETCH_OBJ);

        return false !== $stream;
    }

    public function create(Stream $stream): void
    {
        $streamName = $stream->streamName();

        $this->addStreamToStreamsTable($stream);

        try {
            $tableName = $this->persistenceStrategy->generateTableName($streamName);
            $this->createSchemaFor($tableName);
        } catch (RuntimeException $exception) {
            $this->connection->exec("DROP TABLE $tableName;");
            $this->removeStreamFromStreamsTable($streamName);

            throw $exception;
        }

        $this->connection->beginTransaction();
        $this->duringCreate = true;

        try {
            $this->appendTo($streamName, $stream->streamEvents());
        } catch (\Throwable $e) {
            $this->connection->rollBack();
            $this->duringCreate = false;
            throw $e;
        }

        $this->connection->commit();
        $this->duringCreate = false;
    }

    public function appendTo(StreamName $streamName, Iterator $streamEvents): void
    {
        $data = $this->persistenceStrategy->prepareData($streamEvents);

        if (empty($data)) {
            return;
        }

        $countEntries = iterator_count($streamEvents);
        $columnNames = $this->persistenceStrategy->columnNames();

        $tableName = $this->persistenceStrategy->generateTableName($streamName);

        $rowPlaces = '(' . implode(', ', array_fill(0, count($columnNames), '?')) . ')';
        $allPlaces = implode(', ', array_fill(0, $countEntries, $rowPlaces));

        $sql = 'INSERT INTO ' . $tableName . ' (' . implode(', ', $columnNames) . ') VALUES ' . $allPlaces;

        if (! $this->connection->inTransaction()) {
            $this->connection->beginTransaction();
        }

        $statement = $this->connection->prepare($sql);
        $statement->execute($data);

        if ($statement->errorInfo()[0] === '42S02') {
            if ($this->connection->inTransaction() && ! $this->duringCreate) {
                $this->connection->rollBack();
            }

            throw StreamNotFound::with($streamName);
        }

        if (in_array($statement->errorCode(), $this->persistenceStrategy->uniqueViolationErrorCodes(), true)) {
            if ($this->connection->inTransaction() && ! $this->duringCreate) {
                $this->connection->rollBack();
            }

            throw new ConcurrencyException();
        }

        if ($this->connection->inTransaction() && ! $this->duringCreate) {
            $this->connection->commit();
        }
    }

    public function load(
        StreamName $streamName,
        int $fromNumber = 1,
        int $count = null,
        MetadataMatcher $metadataMatcher = null
    ): Stream {
        if (null === $count) {
            $count = PHP_INT_MAX;
        }

        if (null === $metadataMatcher) {
            $metadataMatcher = new MetadataMatcher();
        }

        $tableName = $this->persistenceStrategy->generateTableName($streamName);

        $sql = [
            'from' => "SELECT * FROM $tableName",
            'orderBy' => 'ORDER BY no ASC',
        ];

        foreach ($metadataMatcher->data() as $match) {
            $field = $match['field'];
            $operator = $match['operator']->getValue();
            $value = $match['value'];

            if (is_bool($value)) {
                $value = var_export($value, true);
            } elseif (is_string($value)) {
                $value = $this->connection->quote($value);
            }

            $sql['where'][] = "metadata->\"$.$field\" $operator $value";
        }

        $limit = $count < $this->loadBatchSize
            ? $count
            : $this->loadBatchSize;

        $query = $sql['from'] . " WHERE no >= $fromNumber";

        if (isset($sql['where'])) {
            $query .= ' AND ';
            $query .= implode(' AND ', $sql['where']);
        }

        $query .= ' ' . $sql['orderBy'];
        $query .= " LIMIT $limit;";

        $statement = $this->connection->prepare($query);
        $statement->setFetchMode(PDO::FETCH_OBJ);
        $statement->execute();

        if (0 === $statement->rowCount()) {
            throw StreamNotFound::with($streamName);
        }

        return new Stream(
            $streamName,
            new PDOStreamIterator(
                $this->connection,
                $statement,
                $this->messageFactory,
                $sql,
                $this->loadBatchSize,
                $fromNumber,
                $count,
                true
            )
        );
    }

    public function loadReverse(
        StreamName $streamName,
        int $fromNumber = PHP_INT_MAX,
        int $count = null,
        MetadataMatcher $metadataMatcher = null
    ): Stream {
        if (null === $count) {
            $count = PHP_INT_MAX;
        }

        if (null === $metadataMatcher) {
            $metadataMatcher = new MetadataMatcher();
        }

        $tableName = $this->persistenceStrategy->generateTableName($streamName);

        $sql = [
            'from' => "SELECT * FROM $tableName",
            'orderBy' => 'ORDER BY no DESC',
        ];

        foreach ($metadataMatcher->data() as $match) {
            $field = $match['field'];
            $operator = $match['operator']->getValue();
            $value = $match['value'];

            if (is_bool($value)) {
                $value = var_export($value, true);
            } elseif (is_string($value)) {
                $value = $this->connection->quote($value);
            }

            $sql['where'][] = "metadata->\"$.$field\" $operator $value";
        }

        $limit = $count < $this->loadBatchSize
            ? $count
            : $this->loadBatchSize;

        $query = $sql['from'] . " WHERE no <= $fromNumber";

        if (isset($sql['where'])) {
            $query .= ' AND ';
            $query .= implode(' AND ', $sql['where']);
        }

        $query .= ' ' . $sql['orderBy'];
        $query .= " LIMIT $limit;";

        $statement = $this->connection->prepare($query);

        $statement->setFetchMode(PDO::FETCH_OBJ);
        $statement->execute();

        if (0 === $statement->rowCount()) {
            throw StreamNotFound::with($streamName);
        }

        return new Stream(
            $streamName,
            new PDOStreamIterator(
                $this->connection,
                $statement,
                $this->messageFactory,
                $sql,
                $this->loadBatchSize,
                $fromNumber,
                $count,
                false
            )
        );
    }

    public function delete(StreamName $streamName): void
    {
        if (! $this->connection->inTransaction()) {
            $this->connection->beginTransaction();
        }

        try {
            $this->removeStreamFromStreamsTable($streamName);
        } catch (StreamNotFound $exception) {
            if ($this->connection->inTransaction()) {
                $this->connection->rollBack();
            }

            throw $exception;
        }

        $encodedStreamName = $this->persistenceStrategy->generateTableName($streamName);

        $deleteEventStreamSql = <<<EOT
DROP TABLE IF EXISTS $encodedStreamName;
EOT;

        $statement = $this->connection->prepare($deleteEventStreamSql);
        $statement->execute();

        if ($this->connection->inTransaction()) {
            $this->connection->commit();
        }
    }

    public function createQuery(): Query
    {
        return new MySQLEventStoreQuery($this, $this->connection, $this->eventStreamsTable);
    }

    public function createProjection(string $name, BaseProjectionOptions $options = null): Projection
    {
        if (null === $options) {
            $options = new ProjectionOptions();
        }

        if (! $options instanceof ProjectionOptions) {
            throw new InvalidArgumentException('options must be an instance of ' . ProjectionOptions::class);
        }

        return new MySQLEventStoreProjection(
            $this,
            $this->connection,
            $name,
            $this->eventStreamsTable,
            $options->projectionsTable(),
            $options->lockTimeoutMs(),
            $options->cacheSize(),
            $options->persistBlockSize()
        );
    }

    public function createReadModelProjection(
        string $name,
        ReadModel $readModel,
        BaseProjectionOptions $options = null
    ): ReadModelProjection {
        if (null === $options) {
            $options = new ProjectionOptions();
        }

        if (! $options instanceof ProjectionOptions) {
            throw new InvalidArgumentException('options must be an instance of ' . ProjectionOptions::class);
        }

        return new MySQLEventStoreReadModelProjection(
            $this,
            $this->connection,
            $name,
            $readModel,
            $this->eventStreamsTable,
            $options->projectionsTable(),
            $options->lockTimeoutMs(),
            $options->cacheSize(),
            $options->persistBlockSize()
        );
    }

    private function addStreamToStreamsTable(Stream $stream): void
    {
        $realStreamName = $stream->streamName()->toString();
        $streamName = $this->persistenceStrategy->generateTableName($stream->streamName());
        $metadata = json_encode($stream->metadata());

        $sql = <<<EOT
INSERT INTO $this->eventStreamsTable (real_stream_name, stream_name, metadata)
VALUES (:realStreamName, :streamName, :metadata);
EOT;

        $statement = $this->connection->prepare($sql);
        $result = $statement->execute([
            ':realStreamName' => $realStreamName,
            ':streamName' => $streamName,
            ':metadata' => $metadata,
        ]);

        if (! $result) {
            if (in_array($statement->errorCode(), $this->persistenceStrategy->uniqueViolationErrorCodes())) {
                throw StreamExistsAlready::with($stream->streamName());
            }
            throw new RuntimeException('Unknown error. Maybe the event streams table is not setup?');
        }
    }

    private function removeStreamFromStreamsTable(StreamName $streamName): void
    {
        $deleteEventStreamTableEntrySql = <<<EOT
DELETE FROM $this->eventStreamsTable WHERE real_stream_name = ?;
EOT;

        $statement = $this->connection->prepare($deleteEventStreamTableEntrySql);
        $statement->execute([$streamName->toString()]);

        if (1 !== $statement->rowCount()) {
            throw StreamNotFound::with($streamName);
        }
    }

    private function createSchemaFor(string $tableName): void
    {
        $schema = $this->persistenceStrategy->createSchema($tableName);

        foreach ($schema as $command) {
            $statement = $this->connection->prepare($command);
            $result = $statement->execute();

            if (! $result) {
                throw new RuntimeException('Error during createSchemaFor: ' . implode('; ', $statement->errorInfo()));
            }
        }
    }
}
