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
use Prooph\EventStore\Exception\ConcurrencyException;
use Prooph\EventStore\Exception\StreamExistsAlready;
use Prooph\EventStore\Exception\StreamNotFound;
use Prooph\EventStore\Exception\TransactionAlreadyStarted;
use Prooph\EventStore\Exception\TransactionNotStarted;
use Prooph\EventStore\Metadata\MetadataMatcher;
use Prooph\EventStore\PDO\Exception\ExtensionNotLoaded;
use Prooph\EventStore\PDO\Exception\InvalidArgumentException;
use Prooph\EventStore\PDO\Exception\RuntimeException;
use Prooph\EventStore\PDO\Projection\PDOEventStoreReadModelProjectionFactory;
use Prooph\EventStore\PDO\Projection\ProjectionOptions;
use Prooph\EventStore\PDO\Projection\PDOEventStoreProjectionFactory;
use Prooph\EventStore\PDO\Projection\PDOEventStoreQueryFactory;
use Prooph\EventStore\Projection\Projection;
use Prooph\EventStore\Projection\ProjectionFactory;
use Prooph\EventStore\Projection\ProjectionOptions as BaseProjectionOptions;
use Prooph\EventStore\Projection\Query;
use Prooph\EventStore\Projection\QueryFactory;
use Prooph\EventStore\Projection\ReadModel;
use Prooph\EventStore\Projection\ReadModelProjection;
use Prooph\EventStore\Projection\ReadModelProjectionFactory;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;
use Prooph\EventStore\TransactionalEventStore;

final class PostgresEventStore implements TransactionalEventStore
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
     * Will be lazy initialized if needed
     *
     * @var QueryFactory
     */
    private $defaultQueryFactory;

    /**
     * Will be lazy initialized if needed
     *
     * @var ProjectionFactory
     */
    private $defaultProjectionFactory;

    /**
     * Will be lazy initialized if needed
     *
     * @var ReadModelProjectionFactory
     */
    private $defaultReadModelProjectionFactory;

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
        if (! extension_loaded('pdo_pgsql')) {
            throw ExtensionNotLoaded::with('pdo_pgsql');
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

        $this->appendTo($streamName, $stream->streamEvents());
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

        $statement = $this->connection->prepare($sql);

        $statement->execute($data);

        if ($statement->errorInfo()[0] === '42P01') {
            throw StreamNotFound::with($streamName);
        }

        if (in_array($statement->errorCode(), $this->persistenceStrategy->uniqueViolationErrorCodes(), true)) {
            throw new ConcurrencyException();
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
                $sql['where'][] = "metadata ->> '$field' $operator '$value'";
            } elseif (is_string($value)) {
                $value = $this->connection->quote($value);
                $sql['where'][] = "metadata ->> '$field' $operator $value";
            } else {
                $sql['where'][] = "metadata ->> '$field' $operator '$value'";
            }
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
                $sql['where'][] = "metadata ->> '$field' $operator '$value'";
            } elseif (is_string($value)) {
                $value = $this->connection->quote($value);
                $sql['where'][] = "metadata ->> '$field' $operator $value";
            } else {
                $sql['where'][] = "metadata ->> '$field' $operator '$value'";
            }
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
        $this->removeStreamFromStreamsTable($streamName);

        $encodedStreamName = $this->persistenceStrategy->generateTableName($streamName);
        $deleteEventStreamSql = <<<EOT
DROP TABLE IF EXISTS $encodedStreamName;
EOT;

        $statement = $this->connection->prepare($deleteEventStreamSql);
        $statement->execute();
    }

    public function beginTransaction(): void
    {
        try {
            $this->connection->beginTransaction();
        } catch (\PDOException $exception) {
            throw new TransactionAlreadyStarted();
        }
    }

    public function commit(): void
    {
        try {
            $this->connection->commit();
        } catch (\PDOException $exception) {
            throw new TransactionNotStarted();
        }
    }

    public function rollback(): void
    {
        try {
            $this->connection->rollBack();
        } catch (\PDOException $exception) {
            throw new TransactionNotStarted();
        }
    }

    public function inTransaction(): bool
    {
        return $this->connection->inTransaction();
    }

    public function transactional(callable $callable)
    {
        $this->beginTransaction();

        try {
            $result = $callable($this);
            $this->commit();
        } catch (\Exception $e) {
            $this->rollback();
            throw $e;
        }

        return $result ?: true;
    }

    public function createQuery(QueryFactory $factory = null): Query
    {
        if (null === $factory) {
            $factory = $this->getDefaultQueryFactory();
        }

        $options = new ProjectionOptions();
        $options->setConnection($this->connection);
        $options->setEventStreamsTable($this->eventStreamsTable);

        return $factory($this, $options);
    }

    public function createProjection(
        string $name,
        BaseProjectionOptions $options = null,
        ProjectionFactory $factory = null
    ): Projection {
        if (null === $factory) {
            $factory = $this->getDefaultProjectionFactory();
        }

        if (null === $options) {
            $options = new ProjectionOptions();
        }

        if (! $options instanceof ProjectionOptions) {
            throw new InvalidArgumentException('options must be an instance of ' . ProjectionOptions::class);
        }

        $options->setConnection($this->connection);
        $options->setEventStreamsTable($this->eventStreamsTable);

        return $factory($this, $name, $options);
    }

    public function createReadModelProjection(
        string $name,
        ReadModel $readModel,
        BaseProjectionOptions $options = null,
        ReadModelProjectionFactory $factory = null
    ): ReadModelProjection {
        if (null === $factory) {
            $factory = $this->getDefaultReadModelProjectionFactory();
        }

        if (null === $options) {
            $options = new ProjectionOptions();
        }

        if (! $options instanceof ProjectionOptions) {
            throw new InvalidArgumentException('options must be an instance of ' . ProjectionOptions::class);
        }

        $options->setConnection($this->connection);
        $options->setEventStreamsTable($this->eventStreamsTable);

        return $factory($this, $name, $readModel, $options);
    }

    public function getDefaultQueryFactory(): QueryFactory
    {
        if (null === $this->defaultQueryFactory) {
            $this->defaultQueryFactory = new PDOEventStoreQueryFactory();
        }

        return $this->defaultQueryFactory;
    }

    public function getDefaultProjectionFactory(): ProjectionFactory
    {
        if (null === $this->defaultProjectionFactory) {
            $this->defaultProjectionFactory = new PDOEventStoreProjectionFactory();
        }

        return $this->defaultProjectionFactory;
    }

    public function getDefaultReadModelProjectionFactory(): ReadModelProjectionFactory
    {
        if (null === $this->defaultReadModelProjectionFactory) {
            $this->defaultReadModelProjectionFactory = new PDOEventStoreReadModelProjectionFactory();
        }

        return $this->defaultReadModelProjectionFactory;
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

            $errorCode = $statement->errorCode();
            $errorInfo = $statement->errorInfo()[2];

            throw new RuntimeException(
                "Error $errorCode. Maybe the event streams table is not setup?\nError-Info: $errorInfo"
            );
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
