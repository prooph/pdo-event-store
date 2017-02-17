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

namespace Prooph\EventStore\Pdo;

use EmptyIterator;
use Iterator;
use PDO;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\Exception\ConcurrencyException;
use Prooph\EventStore\Exception\StreamExistsAlready;
use Prooph\EventStore\Exception\StreamNotFound;
use Prooph\EventStore\Metadata\MetadataMatcher;
use Prooph\EventStore\Pdo\Exception\ExtensionNotLoaded;
use Prooph\EventStore\Pdo\Exception\RuntimeException;
use Prooph\EventStore\Pdo\Projection\PdoEventStoreProjectionFactory;
use Prooph\EventStore\Pdo\Projection\PdoEventStoreQueryFactory;
use Prooph\EventStore\Pdo\Projection\PdoEventStoreReadModelProjectionFactory;
use Prooph\EventStore\Pdo\Projection\ProjectionStatus;
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
use Prooph\EventStore\Util\Assertion;

final class MySqlEventStore implements EventStore
{
    /**
     * @var MessageFactory
     */
    private $messageFactory;

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
     * @var string
     */
    private $projectionsTable;

    /**
     * @var bool
     */
    private $duringCreate = false;

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
        PDO $connection,
        PersistenceStrategy $persistenceStrategy,
        int $loadBatchSize = 10000,
        string $eventStreamsTable = 'event_streams',
        string $projectionsTable = 'projections'
    ) {
        if (! extension_loaded('pdo_mysql')) {
            throw ExtensionNotLoaded::with('pdo_mysql');
        }

        Assertion::min($loadBatchSize, 1);

        $this->messageFactory = $messageFactory;
        $this->connection = $connection;
        $this->persistenceStrategy = $persistenceStrategy;
        $this->loadBatchSize = $loadBatchSize;
        $this->eventStreamsTable = $eventStreamsTable;
        $this->projectionsTable = $projectionsTable;
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
SELECT COUNT(1) FROM $this->eventStreamsTable
WHERE real_stream_name = :streamName;
EOT;

        $statement = $this->connection->prepare($sql);

        $statement->execute(['streamName' => $streamName->toString()]);

        return '1' === $statement->fetchColumn();
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
    ): Iterator {
        if (null === $count) {
            $count = PHP_INT_MAX;
        }

        [$where, $values] = $this->createWhereClauseForMetadata($metadataMatcher);
        $where[] = '`no` >= :fromNumber';

        $whereCondition = 'WHERE ' . implode(' AND ', $where);
        $limit = min($count, $this->loadBatchSize);

        $tableName = $this->persistenceStrategy->generateTableName($streamName);

        $query = <<<EOT
SELECT * FROM $tableName
$whereCondition
ORDER BY `no` ASC
LIMIT :limit;
EOT;

        $statement = $this->connection->prepare($query);
        $statement->setFetchMode(PDO::FETCH_OBJ);

        $statement->bindValue(':fromNumber', $fromNumber, PDO::PARAM_INT);
        $statement->bindValue(':limit', $limit, PDO::PARAM_INT);

        foreach ($values as $parameter => $value) {
            $statement->bindValue($parameter, $value, is_int($value) ? PDO::PARAM_INT : PDO::PARAM_STR);
        }

        $statement->execute();

        if ($statement->errorCode() !== '00000') {
            throw StreamNotFound::with($streamName);
        }

        if (0 === $statement->rowCount()) {
            return new EmptyIterator();
        }

        return new PdoStreamIterator(
            $this->connection,
            $statement,
            $this->messageFactory,
            $this->loadBatchSize,
            $fromNumber,
            $count,
            true
        );
    }

    public function loadReverse(
        StreamName $streamName,
        int $fromNumber = PHP_INT_MAX,
        int $count = null,
        MetadataMatcher $metadataMatcher = null
    ): Iterator {
        if (null === $count) {
            $count = PHP_INT_MAX;
        }

        [$where, $values] = $this->createWhereClauseForMetadata($metadataMatcher);
        $where[] = '`no` <= :fromNumber';

        $whereCondition = 'WHERE ' . implode(' AND ', $where);
        $limit = min($count, $this->loadBatchSize);

        $tableName = $this->persistenceStrategy->generateTableName($streamName);

        $query = <<<EOT
SELECT * FROM $tableName
$whereCondition
ORDER BY `no` DESC
LIMIT :limit;
EOT;

        $statement = $this->connection->prepare($query);
        $statement->setFetchMode(PDO::FETCH_OBJ);

        $statement->bindValue(':fromNumber', $fromNumber, PDO::PARAM_INT);
        $statement->bindValue(':limit', $limit, PDO::PARAM_INT);

        foreach ($values as $parameter => $value) {
            $statement->bindValue($parameter, $value, is_int($value) ? PDO::PARAM_INT : PDO::PARAM_STR);
        }

        $statement->execute();

        if ($statement->errorCode() !== '00000') {
            throw StreamNotFound::with($streamName);
        }

        if (0 === $statement->rowCount()) {
            return new EmptyIterator();
        }

        return new PdoStreamIterator(
            $this->connection,
            $statement,
            $this->messageFactory,
            $this->loadBatchSize,
            $fromNumber,
            $count,
            false
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

    public function createQuery(QueryFactory $factory = null): Query
    {
        if (null === $factory) {
            $factory = $this->getDefaultQueryFactory();
        }

        return $factory($this);
    }

    public function createProjection(
        string $name,
        BaseProjectionOptions $options = null,
        ProjectionFactory $factory = null
    ): Projection {
        if (null === $factory) {
            $factory = $this->getDefaultProjectionFactory();
        }

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

        return $factory($this, $name, $readModel, $options);
    }

    public function getDefaultQueryFactory(): QueryFactory
    {
        if (null === $this->defaultQueryFactory) {
            $this->defaultQueryFactory = new PdoEventStoreQueryFactory(
                $this->connection,
                $this->eventStreamsTable
            );
        }

        return $this->defaultQueryFactory;
    }

    public function getDefaultProjectionFactory(): ProjectionFactory
    {
        if (null === $this->defaultProjectionFactory) {
            $this->defaultProjectionFactory = new PdoEventStoreProjectionFactory(
                $this->connection,
                $this->eventStreamsTable,
                $this->projectionsTable
            );
        }

        return $this->defaultProjectionFactory;
    }

    public function getDefaultReadModelProjectionFactory(): ReadModelProjectionFactory
    {
        if (null === $this->defaultReadModelProjectionFactory) {
            $this->defaultReadModelProjectionFactory = new PdoEventStoreReadModelProjectionFactory(
                $this->connection,
                $this->eventStreamsTable,
                $this->projectionsTable
            );
        }

        return $this->defaultReadModelProjectionFactory;
    }

    public function deleteProjection(string $name, bool $deleteEmittedEvents): void
    {
        $sql = <<<EOT
UPDATE $this->projectionsTable SET status = ? WHERE name = ? LIMIT 1;
EOT;

        if ($deleteEmittedEvents) {
            $status = ProjectionStatus::DELETING_INCL_EMITTED_EVENTS()->getValue();
        } else {
            $status = ProjectionStatus::DELETING()->getValue();
        }

        $statement = $this->connection->prepare($sql);
        $statement->execute([
            $status,
            $name,
        ]);
    }

    public function resetProjection(string $name): void
    {
        $sql = <<<EOT
UPDATE $this->projectionsTable SET status = ? WHERE name = ? LIMIT 1;
EOT;

        $statement = $this->connection->prepare($sql);
        $statement->execute([
            ProjectionStatus::RESETTING()->getValue(),
            $name,
        ]);
    }

    public function stopProjection(string $name): void
    {
        $sql = <<<EOT
UPDATE $this->projectionsTable SET status = ? WHERE name = ? LIMIT 1;
EOT;

        $statement = $this->connection->prepare($sql);
        $statement->execute([
            ProjectionStatus::STOPPING()->getValue(),
            $name,
        ]);
    }

    public function fetchStreamNames(
        ?string $filter,
        bool $regex,
        ?MetadataMatcher $metadataMatcher,
        int $limit,
        int $offset
    ): array {
        if (null === $filter && $regex) {
            throw new Exception\InvalidArgumentException('No regex pattern given');
        }

        if ($regex && false === @preg_match("/$filter/", '')) {
            throw new Exception\InvalidArgumentException('Invalid regex pattern given');
        }

        [$where, $values] = $this->createWhereClauseForMetadata($metadataMatcher);

        if (null !== $filter && $regex) {
            $where[] = '`real_stream_name` REGEXP :filter ';
            $values[':filter'] = $filter;
        } elseif (null !== $filter && ! $regex) {
            $where[] = '`real_stream_name` = :filter ';
            $values[':filter'] = $filter;
        }

        $whereCondition = implode(' AND ', $where);
        if (! empty($whereCondition)) {
            $whereCondition = 'WHERE ' . $whereCondition;
        }

        $query = <<<SQL
SELECT `real_stream_name` FROM $this->eventStreamsTable
$whereCondition
ORDER BY `real_stream_name` ASC
LIMIT $offset, $limit
SQL;

        $statement = $this->connection->prepare($query);
        $statement->setFetchMode(PDO::FETCH_OBJ);
        $statement->execute($values);

        if ($statement->errorCode() !== '00000') {
            $errorCode = $statement->errorCode();
            $errorInfo = $statement->errorInfo()[2];

            throw new RuntimeException(
                "Error $errorCode. Maybe the event streams table is not setup?\nError-Info: $errorInfo"
            );
        }

        $result = $statement->fetchAll();

        $streamNames = [];

        foreach ($result as $streamName) {
            $streamNames[] = new StreamName($streamName->real_stream_name);
        }

        return $streamNames;
    }

    public function fetchCategoryNames(?string $filter, bool $regex, int $limit, int $offset): array
    {
        if (null === $filter && $regex) {
            throw new Exception\InvalidArgumentException('No regex pattern given');
        }

        if ($regex && false === @preg_match("/$filter/", '')) {
            throw new Exception\InvalidArgumentException('Invalid regex pattern given');
        }

        $where = [];
        $values = [];

        if (null !== $filter && $regex) {
            $where[] = '`category` REGEXP :filter ';
            $values[':filter'] = $filter;
        } elseif (null !== $filter && ! $regex) {
            $where[] = '`category` = :filter ';
            $values[':filter'] = $filter;
        }

        $whereCondition = implode(' AND ', $where);
        if (! empty($whereCondition)) {
            $whereCondition = 'WHERE ' . $whereCondition . ' AND `category` IS NOT NULL';
        } else {
            $whereCondition = 'WHERE `category` IS NOT NULL';
        }

        $query = <<<SQL
SELECT `category` FROM $this->eventStreamsTable
$whereCondition
GROUP BY `category`
ORDER BY `category` ASC
LIMIT $offset, $limit
SQL;

        $statement = $this->connection->prepare($query);
        $statement->setFetchMode(PDO::FETCH_OBJ);
        $statement->execute($values);

        if ($statement->errorCode() !== '00000') {
            $errorCode = $statement->errorCode();
            $errorInfo = $statement->errorInfo()[2];

            throw new RuntimeException(
                "Error $errorCode. Maybe the event streams table is not setup?\nError-Info: $errorInfo"
            );
        }

        $result = $statement->fetchAll();

        $categoryNames = [];

        foreach ($result as $categoryName) {
            $categoryNames[] = $categoryName->category;
        }

        return $categoryNames;
    }

    public function fetchProjectionNames(?string $filter, bool $regex, int $limit, int $offset): array
    {
        if (null === $filter && $regex) {
            throw new Exception\InvalidArgumentException('No regex pattern given');
        }

        if ($regex && false === @preg_match("/$filter/", '')) {
            throw new Exception\InvalidArgumentException('Invalid regex pattern given');
        }

        $where = [];
        $values = [];

        if (null !== $filter && $regex) {
            $where[] = '`name` REGEXP :filter ';
            $values[':filter'] = $filter;
        } elseif (null !== $filter && ! $regex) {
            $where[] = '`name` = :filter ';
            $values[':filter'] = $filter;
        }

        $whereCondition = implode(' AND ', $where);
        if (! empty($whereCondition)) {
            $whereCondition = 'WHERE ' . $whereCondition;
        }

        $query = <<<SQL
SELECT `name` FROM $this->$this->projectionsTable
$whereCondition
ORDER BY `name` ASC
LIMIT $offset, $limit
SQL;

        $statement = $this->connection->prepare($query);
        $statement->setFetchMode(PDO::FETCH_OBJ);
        $statement->execute($values);

        if ($statement->errorCode() !== '00000') {
            $errorCode = $statement->errorCode();
            $errorInfo = $statement->errorInfo()[2];

            throw new RuntimeException(
                "Error $errorCode. Maybe the event streams table is not setup?\nError-Info: $errorInfo"
            );
        }

        $result = $statement->fetchAll();

        $projectionNames = [];

        foreach ($result as $projectionName) {
            $projectionNames[] = $projectionName->name;
        }

        return $projectionNames;
    }

    private function createWhereClauseForMetadata(?MetadataMatcher $metadataMatcher): array
    {
        $where = [];
        $values = [];

        if (! $metadataMatcher) {
            return [
                $where,
                $values,
            ];
        }

        foreach ($metadataMatcher->data() as $key => $match) {
            $field = $match['field'];
            $operator = $match['operator']->getValue();
            $value = $match['value'];
            $parameter = ':metadata_'.$key;

            if (is_bool($value)) {
                $where[] = "metadata->\"$.$field\" $operator ".var_export($value, true);
                continue;
            }

            $where[] = "metadata->\"$.$field\" $operator $parameter";
            $values[$parameter] = $value;
        }

        return [
            $where,
            $values,
        ];
    }

    private function addStreamToStreamsTable(Stream $stream): void
    {
        $realStreamName = $stream->streamName()->toString();

        $pos = strpos($realStreamName, '-');

        if (false !== $pos && $pos > 0) {
            $category = substr($realStreamName, 0, $pos);
        } else {
            $category = null;
        }

        $streamName = $this->persistenceStrategy->generateTableName($stream->streamName());
        $metadata = json_encode($stream->metadata(), \JSON_FORCE_OBJECT);

        $sql = <<<EOT
INSERT INTO $this->eventStreamsTable (real_stream_name, stream_name, metadata, category)
VALUES (:realStreamName, :streamName, :metadata, :category);
EOT;

        $statement = $this->connection->prepare($sql);
        $result = $statement->execute([
            ':realStreamName' => $realStreamName,
            ':streamName' => $streamName,
            ':metadata' => $metadata,
            ':category' => $category,
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
