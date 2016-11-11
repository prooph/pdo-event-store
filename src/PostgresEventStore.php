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

use PDO;
use Prooph\Common\Event\ActionEvent;
use Prooph\Common\Event\ActionEventEmitter;
use Prooph\Common\Messaging\MessageConverter;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\EventStore\AbstractCanControlTransactionActionEventEmitterAwareEventStore;
use Prooph\EventStore\Exception\ConcurrencyException;
use Prooph\EventStore\Exception\StreamNotFound;
use Prooph\EventStore\Metadata\MetadataMatcher;
use Prooph\EventStore\PDO\Exception\ExtensionNotLoaded;
use Prooph\EventStore\PDO\Exception\RuntimeException;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;

final class PostgresEventStore extends AbstractCanControlTransactionActionEventEmitterAwareEventStore
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
     * @throws ExtensionNotLoaded
     */
    public function __construct(
        ActionEventEmitter $actionEventEmitter,
        MessageFactory $messageFactory,
        MessageConverter $messageConverter,
        PDO $connection,
        IndexingStrategy $indexingStrategy,
        TableNameGeneratorStrategy $tableNameGeneratorStrategy,
        int $loadBatchSize = 10000,
        string $eventStreamsTable = 'event_streams'
    ) {
        if (! extension_loaded('pdo_pgsql')) {
            throw ExtensionNotLoaded::with('pdo_pgsql');
        }

        $this->actionEventEmitter = $actionEventEmitter;
        $this->messageFactory = $messageFactory;
        $this->messageConverter = $messageConverter;
        $this->connection = $connection;
        $this->indexingStrategy = $indexingStrategy;
        $this->tableNameGeneratorStrategy = $tableNameGeneratorStrategy;
        $this->loadBatchSize = $loadBatchSize;
        $this->eventStreamsTable = $eventStreamsTable;

        $actionEventEmitter->attachListener(self::EVENT_CREATE, function (ActionEvent $event): void {
            $stream = $event->getParam('stream');

            $streamName = $stream->streamName();

            $this->createSchemaFor($streamName);
            $this->addStreamToStreamsTable($stream);

            $this->appendTo($streamName, $stream->streamEvents());

            $event->setParam('result', true);
        });

        $actionEventEmitter->attachListener(self::EVENT_APPEND_TO, function (ActionEvent $event): void {
            $streamName = $event->getParam('streamName');
            $streamEvents = $event->getParam('streamEvents');

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
                    $data[] = $streamEvent->metadata()['_aggregate_version'];
                }
            }

            $tableName = $this->tableNameGeneratorStrategy->__invoke($streamName);

            $rowPlaces = '(' . implode(', ', array_fill(0, count($columnNames), '?')) . ')';
            $allPlaces = implode(', ', array_fill(0, $countEntries, $rowPlaces));

            $sql = 'INSERT INTO ' . $tableName . ' (' . implode(', ', $columnNames) . ') VALUES ' . $allPlaces;

            $statement = $this->connection->prepare($sql);

            $result = $statement->execute($data);

            if (in_array($statement->errorCode(), $this->indexingStrategy->uniqueViolationErrorCodes(), true)) {
                throw new ConcurrencyException();
            }

            if (! $result) {
                throw new RuntimeException('Error during appendTo: ' . implode('; ', $statement->errorInfo()));
            }

            $event->setParam('result', true);
        });

        $actionEventEmitter->attachListener(self::EVENT_LOAD, function (ActionEvent $event): void {
            $streamName = $event->getParam('streamName');
            $fromNumber = $event->getParam('fromNumber');
            $count = $event->getParam('count');
            $metadataMatcher = $event->getParam('metadataMatcher');

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
                $sql['where'][] = "metadata->>'$key' $operator $value";
            }

            if (null === $count) {
                $count = PHP_INT_MAX;
            }

            $limit = $count < $this->loadBatchSize
                ? $count
                : $this->loadBatchSize;

            $query = $sql['from'] . " WHERE no >= $fromNumber";

            if (isset($sql['where'])) {
                $query .= 'AND ';
                $query .= implode(' AND ', $sql['where']);
            }
            $query .= ' ' . $sql['orderBy'];
            $query .= " LIMIT $limit;";

            $statement = $this->connection->prepare($query);
            $statement->setFetchMode(PDO::FETCH_OBJ);
            $statement->execute();

            if (0 === $statement->rowCount()) {
                $event->setParam('stream', false);
                return;
            }

            $event->setParam('stream', new Stream(
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
            ));
        });

        $actionEventEmitter->attachListener(self::EVENT_LOAD_REVERSE, function (ActionEvent $event): void {
            $streamName = $event->getParam('streamName');
            $fromNumber = $event->getParam('fromNumber');
            $count = $event->getParam('count');
            $metadataMatcher = $event->getParam('metadataMatcher');

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
                $sql['where'][] = "metadata->>'$key' $operator $value";
            }

            $event->setParam('stream', new Stream(
                $streamName,
                new PDOStreamIterator(
                    $this->connection,
                    $this->messageFactory,
                    $sql,
                    $this->loadBatchSize,
                    $fromNumber,
                    $count,
                    false
                )
            ));
        });

        $this->actionEventEmitter->attachListener(self::EVENT_BEGIN_TRANSACTION, function (ActionEvent $event): void {
            $this->connection->beginTransaction();

            $event->setParam('inTransaction', true);
        });

        $this->actionEventEmitter->attachListener(self::EVENT_COMMIT, function (ActionEvent $event): void {
            $this->connection->commit();

            $event->setParam('inTransaction', false);
        });

        $this->actionEventEmitter->attachListener(self::EVENT_ROLLBACK, function (ActionEvent $event): void {
            $this->connection->rollBack();

            $event->setParam('inTransaction', false);
        });
    }

    public function hasStream(StreamName $streamName): bool
    {
        $eventStreamsTable = $this->eventStreamsTable;

        $sql = <<<EOT
SELECT metadata FROM $eventStreamsTable
WHERE real_stream_name = :streamName'; 
EOT;
        $statement = $this->connection->prepare($sql);
        $result = $statement->execute(['streamName' => $streamName->toString()]);

        if (! $result) {
            throw new RuntimeException('Error during fetchStreamMetadata: ' . implode('; ', $statement->errorInfo()));
        }

        $stream = $statement->fetch(PDO::FETCH_OBJ);

        if (null === $stream) {
            throw StreamNotFound::with($streamName);
        }

        return true;
    }

    public function fetchStreamMetadata(StreamName $streamName): array
    {
        $eventStreamsTable = $this->eventStreamsTable;

        $sql = <<<EOT
SELECT metadata FROM $eventStreamsTable
WHERE real_stream_name = :streamName'; 
EOT;
        $statement = $this->connection->prepare($sql);
        $result = $statement->execute(['streamName' => $streamName->toString()]);

        if (! $result) {
            throw new RuntimeException('Error during fetchStreamMetadata: ' . implode('; ', $statement->errorInfo()));
        }

        $stream = $statement->fetch(PDO::FETCH_OBJ);

        if (null === $stream) {
            throw StreamNotFound::with($streamName);
        }

        return json_decode($stream->metadata, true);
    }

    private function addStreamToStreamsTable(Stream $stream): void
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
            throw new RuntimeException('Error during addStreamToStreamsTable: ' . implode('; ', $statement->errorInfo()));
        }
    }

    private function createSchemaFor(StreamName $streamName): void
    {
        $tableName = $this->tableNameGeneratorStrategy->__invoke($streamName);
        $schema = $this->indexingStrategy->createSchema($tableName);

        foreach ($schema as $command) {
            $statement = $this->connection->prepare($command);
            $result = $statement->execute();

            if (! $result) {
                throw new RuntimeException('Error during createSchemaFor: ' . implode('; ', $statement->errorInfo()));
            }
        }
    }
}
