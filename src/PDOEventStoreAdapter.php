<?php
/**
 * This file is part of the prooph/event-store-pdo-adapter.
 * (c) 2016-2016 prooph software GmbH <contact@prooph.de>
 * (c) 2016-2016 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Prooph\EventStore\Adapter\PDO;

use Iterator;
use PDO;
use Prooph\Common\Messaging\MessageConverter;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\EventStore\Adapter\Adapter;
use Prooph\EventStore\Adapter\Exception\RuntimeException;
use Prooph\EventStore\Adapter\Feature\CanHandleTransaction;
use Prooph\EventStore\Exception\ConcurrencyException;
use Prooph\EventStore\Metadata\MetadataMatcher;
use Prooph\EventStore\Stream\Stream;
use Prooph\EventStore\Stream\StreamName;

final class PDOEventStoreAdapter implements Adapter, CanHandleTransaction
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
     * @var JsonQuerier
     */
    private $jsonQuerier;

    /**
     * @var IndexingStrategy
     */
    private $indexingStrategy;

    /**
     * @var TableNameGeneratorStrategy
     */
    private $tableNameGeneratorStrategy;

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
    private $inTransaction = false;

    public function __construct(
        MessageFactory $messageFactory,
        MessageConverter $messageConverter,
        PDO $connection,
        JsonQuerier $jsonQuerier,
        IndexingStrategy $indexingStrategy,
        TableNameGeneratorStrategy $tableNameGeneratorStrategy,
        int $loadBatchSize = 10000,
        string $eventStreamsTable = 'event_streams'
    ) {
        $this->messageFactory             = $messageFactory;
        $this->messageConverter           = $messageConverter;
        $this->connection                 = $connection;
        $this->jsonQuerier                = $jsonQuerier;
        $this->indexingStrategy           = $indexingStrategy;
        $this->tableNameGeneratorStrategy = $tableNameGeneratorStrategy;
        $this->loadBatchSize              = $loadBatchSize;
        $this->eventStreamsTable          = $eventStreamsTable;
    }

    public function hasStream(StreamName $streamName): bool
    {
        $eventStreamsTable = $this->eventStreamsTable;
        $streamName = $streamName->toString();

        $sql = <<<EOT
SELECT metadata FROM $eventStreamsTable
WHERE real_stream_name = :streamName'; 
EOT;
        $statement = $this->connection->prepare($sql);
        $result = $statement->execute(['streamName' => $streamName]);

        if (! $result) {
            throw new RuntimeException('Error during fetchStreamMetadata: ' . join('; ', $statement->errorInfo()));
        }

        $stream = $statement->fetch(PDO::FETCH_OBJ);

        if (null === $stream) {
            return false;
        }

        return true;
    }

    public function fetchStreamMetadata(StreamName $streamName): ?array
    {
        $eventStreamsTable = $this->eventStreamsTable;
        $streamName = $streamName->toString();

        $sql = <<<EOT
SELECT metadata FROM $eventStreamsTable
WHERE real_stream_name = :streamName'; 
EOT;
        $statement = $this->connection->prepare($sql);
        $result = $statement->execute(['streamName' => $streamName]);

        if (! $result) {
            throw new RuntimeException('Error during fetchStreamMetadata: ' . join('; ', $statement->errorInfo()));
        }

        $stream = $statement->fetch(PDO::FETCH_OBJ);

        if (null === $stream) {
            return null;
        }

        return json_decode($stream->metadata, true);
    }

    public function create(Stream $stream): void
    {
        $streamName = $stream->streamName();

        $this->addStreamToStreamsTable($stream);
        $this->createSchemaFor($streamName);

        $this->appendTo($streamName, $stream->streamEvents());
    }

    public function appendTo(StreamName $streamName, Iterator $streamEvents): void
    {
        $columnNames = [
            'event_id',
            'event_name',
            'payload',
            'metadata',
            'created_at'
        ];

        if ($this->indexingStrategy->oneStreamPerAggregate()) {
            $columnNames[] = 'no';
        }

        $data = [];
        $countEntries = 0;

        foreach ($streamEvents as $streamEvent) {
            $countEntries++;
            $data[] = $streamEvent->uuid()->toString();
            $data[] = $streamEvent->messageName();
            $data[] = json_encode($streamEvent->payload());
            $data[] = json_encode($streamEvent->metadata());
            $data[] = $streamEvent->createdAt()->format('Y-m-d\TH:i:s.u');

            if ($this->indexingStrategy->oneStreamPerAggregate()) {
                $data[] = $streamEvent->metadata()['_version'];
            }
        }

        $tableName = $this->tableNameGeneratorStrategy->__invoke($streamName);

        $rowPlaces = '(' . implode(', ', array_fill(0, count($columnNames), '?')) . ')';
        $allPlaces = implode(', ', array_fill(0, $countEntries, $rowPlaces));

        $sql = 'INSERT INTO ' . $tableName . ' (' . implode(', ', $columnNames) . ') VALUES ' . $allPlaces;

        $statement = $this->connection->prepare($sql);

        $result = $statement->execute($data);

        if ($statement->errorCode() === $this->indexingStrategy->uniqueViolationErrorCode()) {
            throw new ConcurrencyException();
        }

        if (! $result) {
            throw new RuntimeException('Error during appendTo: ' . join('; ', $statement->errorInfo()));
        }
    }

    public function load(
        StreamName $streamName,
        int $fromNumber = 0,
        int $count = null
    ): ?Stream {
        $events = $this->loadEvents($streamName, $fromNumber, $count);

        return new Stream($streamName, $events);
    }

    public function loadReverse(
        StreamName $streamName,
        int $fromNumber = PHP_INT_MAX,
        int $count = null
    ): ?Stream {
        $events = $this->loadEventsReverse($streamName, $fromNumber, $count);

        return new Stream($streamName, $events);
    }

    public function loadEvents(
        StreamName $streamName,
        int $fromNumber = 0,
        int $count = null,
        MetadataMatcher $metadataMatcher = null
    ): Iterator {
        if (null === $metadataMatcher) {
            $metadataMatcher = new MetadataMatcher();
        }

        $tableName = $this->tableNameGeneratorStrategy->__invoke($streamName);
        $sql = [
            'from' => "SELECT * FROM $tableName",
            'orderBy' => "ORDER BY no ASC",
        ];

        foreach ($metadataMatcher->data() as $key => $v) {
            $operator = $v['operator']->getValue();
            $value = $v['value'];
            if (is_string($value)) {
                $value = "'$value'";
            }
            $sql['where'][] = $this->jsonQuerier->metadata($key) . " $operator $value";
        }

        return new PDOStreamIterator($this->connection, $this->messageFactory, $sql, $this->loadBatchSize, $fromNumber, $count);
    }

    public function loadEventsReverse(
        StreamName $streamName,
        int $fromNumber = PHP_INT_MAX,
        int $count = null,
        MetadataMatcher $metadataMatcher = null
    ): Iterator {
        if (null === $metadataMatcher) {
            $metadataMatcher = new MetadataMatcher();
        }

        $tableName = $this->tableNameGeneratorStrategy->__invoke($streamName);
        $sql = [
            'from' => "SELECT * FROM $tableName",
            'orderBy' => "ORDER BY no DESC",
        ];

        foreach ($metadataMatcher->data() as $key => $v) {
            $operator = $v['operator']->getValue();
            $value = $v['value'];
            if (is_string($value)) {
                $value = "'$value'";
            }
            $sql['where'][] = $this->jsonQuerier->metadata($key) . " $operator $value";
        }

        return new PDOStreamIterator($this->connection, $this->messageFactory, $sql, $this->loadBatchSize, $fromNumber, $count);
    }

    /**
     * @throws RuntimeException
     */
    public function beginTransaction(): void
    {
        if ($this->inTransaction) {
            throw new RuntimeException('Transaction already started');
        }

        $this->inTransaction = true;
        $this->connection->beginTransaction();
    }

    public function commit(): void
    {
        $this->connection->commit();
        $this->inTransaction = false;
    }

    public function rollback(): void
    {
        if (! $this->inTransaction) {
            return;
        }

        $this->connection->rollBack();
        $this->inTransaction = false;
    }

    public function addStreamToStreamsTable(Stream $stream): void
    {
        $realStreamName = $stream->streamName()->toString();
        $streamName = $this->tableNameGeneratorStrategy->__invoke($stream->streamName());
        $metadata = json_encode($stream->metadata());

        $sql = <<<EOT
INSERT INTO $this->eventStreamsTable (real_stream_name, stream_name, metadata)
VALUES (:realStreamName, :streamName, :metadata);
EOT;

        $statement = $this->connection->prepare($sql);
        $result = $statement->execute([
            ':realStreamName' => $realStreamName,
            ':streamName' => $streamName,
            ':metadata' => $metadata
        ]);

        if (! $result) {
            throw new RuntimeException('Error during addStreamToStreamsTable: ' . join('; ', $statement->errorInfo()));
        }
    }

    public function createSchemaFor(StreamName $streamName): void
    {
        $schema = $this->getSqlSchemaFor($streamName);
        $statement = $this->connection->prepare($schema);
        $result = $statement->execute();

        if (! $result) {
            throw new RuntimeException('Error during createSchemaFor: ' . join('; ', $statement->errorInfo()));
        }
    }

    public function getSqlSchemaFor(StreamName $streamName)
    {
        $tableName = $this->tableNameGeneratorStrategy->__invoke($streamName);
        return $this->indexingStrategy->createSchema($tableName);
    }
}
