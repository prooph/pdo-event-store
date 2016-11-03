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
        IndexingStrategy $indexingStrategy,
        TableNameGeneratorStrategy $tableNameGeneratorStrategy,
        int $loadBatchSize = 10000,
        string $eventStreamsTable = 'event_streams'
    ) {
        $this->messageFactory             = $messageFactory;
        $this->messageConverter           = $messageConverter;
        $this->connection                 = $connection;
        $this->indexingStrategy           = $indexingStrategy;
        $this->tableNameGeneratorStrategy = $tableNameGeneratorStrategy;
        $this->loadBatchSize              = $loadBatchSize;
        $this->eventStreamsTable          = $eventStreamsTable;
    }

    public function fetchStreamMetadata(StreamName $streamName): ?array
    {
        $eventStreamsTable = $this->eventStreamsTable;
        $streamName = $streamName->toString();

        $sql = <<<EOT
SELECT `metadata` FROM `$eventStreamsTable`
WHERE `real_stream_name` = '$streamName'; 
EOT;
        $statement = $this->connection->query($sql);
        $statement->execute();

        $result = $statement->fetch(PDO::FETCH_OBJ);

        if (null === $result) {
            return null;
        }

        return json_decode($result->metadata);
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
        $data = [];

        foreach ($streamEvents as $streamEvent) {
            $data[] = '(\'' . $streamEvent->uuid()->toString() . '\', '
                . '\'' . $streamEvent->messageName() . '\', '
                . '\'' . json_encode($streamEvent->payload()) . '\', '
                . '\'' . json_encode($streamEvent->metadata()) . '\', '
                . '\'' . $streamEvent->createdAt()->format('Y-m-d\TH:i:s.u') . '\')';
        }

        $data = implode(', ' . $data);

        $tableName = $this->tableNameGeneratorStrategy->__invoke($streamName);

        $sql = <<<EOT
INSERT INTO `$tableName` (`event_id`, `event_name`, `payload`, `metadata`, `created_at`)
VALUES $data;
EOT;

        $this->connection->exec($sql);
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
        $tableName = $this->tableNameGeneratorStrategy->__invoke($streamName);
        $sql = [
            'from' => "SELECT * FROM `$tableName`",
            'orderBy' => "ORDER BY `no` ASC",
        ];

        foreach ($metadataMatcher->data() as $key => $v) {
            $operator = $v['operator']->getValue();
            $value = $v['value'];
            if (is_string($value)) {
                $value = "'$value'";
            }
            $sql['where'][] = "`$key`` $operator $value";
        }

        return new PDOStreamIterator($this->connection, $this->messageFactory, $sql, $this->loadBatchSize, $fromNumber, $count);
    }

    public function loadEventsReverse(
        StreamName $streamName,
        int $fromNumber = PHP_INT_MAX,
        int $count = null,
        MetadataMatcher $metadataMatcher = null
    ): Iterator {
        $tableName = $this->tableNameGeneratorStrategy->__invoke($streamName);
        $sql = [
            'from' => "SELECT * FROM `$tableName`",
            'orderBy' => "ORDER BY `no` DESC",
        ];

        foreach ($metadataMatcher->data() as $key => $v) {
            $operator = $v['operator']->getValue();
            $value = $v['value'];
            if (is_string($value)) {
                $value = "'$value'";
            }
            $sql['where'][] = "`$key`` $operator $value";
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
INSERT INTO `$this->eventStreamsTable` (`real_stream_name`, `stream_name`, `metadata`)
VALUES ('$realStreamName', '$streamName', '$metadata');
EOT;

        $this->connection->exec($sql);
    }

    public function createSchemaFor(StreamName $streamName): void
    {
        $schema = $this->getSqlSchemaFor($streamName);
        $this->connection->exec($schema);
    }

    public function getSqlSchemaFor(StreamName $streamName)
    {
        $tableName = $this->tableNameGeneratorStrategy->__invoke($streamName);
        return $this->indexingStrategy->__invoke($tableName);
    }
}
