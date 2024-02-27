<?php

/**
 * This file is part of prooph/pdo-event-store.
 * (c) 2016-2022 Alexander Miertsch <kontakt@codeliner.ws>
 * (c) 2016-2022 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Prooph\EventStore\Pdo\PersistenceStrategy;

use Iterator;
use Prooph\Common\Messaging\MessageConverter;
use Prooph\EventStore\Pdo\DefaultMessageConverter;
use Prooph\EventStore\Pdo\Util\Json;
use Prooph\EventStore\Pdo\Util\PostgresHelper;
use Prooph\EventStore\StreamName;

final class PostgresSingleStreamStrategy implements PostgresPersistenceStrategy
{
    use PostgresHelper;

    /**
     * @var MessageConverter
     */
    private $messageConverter;

    public function __construct(?MessageConverter $messageConverter = null)
    {
        $this->messageConverter = $messageConverter ?? new DefaultMessageConverter();
    }

    /**
     * @param string $tableName
     * @return string[]
     */
    public function createSchema(string $tableName): array
    {
        $tableName = $this->quoteIdent($tableName);

        $statement = <<<EOT
CREATE TABLE $tableName (
    no BIGSERIAL,
    event_id UUID NOT NULL,
    event_name VARCHAR(100) NOT NULL,
    payload JSON NOT NULL,
    metadata JSONB NOT NULL,
    created_at TIMESTAMP(6) NOT NULL,
    PRIMARY KEY (no),
    CONSTRAINT aggregate_version_not_null CHECK ((metadata->>'_aggregate_version') IS NOT NULL),
    CONSTRAINT aggregate_type_not_null CHECK ((metadata->>'_aggregate_type') IS NOT NULL),
    CONSTRAINT aggregate_id_not_null CHECK ((metadata->>'_aggregate_id') IS NOT NULL),
    UNIQUE (event_id)
);
EOT;

        $index1 = <<<EOT
CREATE UNIQUE INDEX ON $tableName
((metadata->>'_aggregate_type'), (metadata->>'_aggregate_id'), (metadata->>'_aggregate_version'));
EOT;

        $index2 = <<<EOT
CREATE INDEX ON $tableName
((metadata->>'_aggregate_type'), (metadata->>'_aggregate_id'), no);
EOT;

        return \array_merge($this->getSchemaCreationSchema($tableName), [
            $statement,
            $index1,
            $index2,
        ]);
    }

    public function columnNames(): array
    {
        return [
            'event_id',
            'event_name',
            'payload',
            'metadata',
            'created_at',
        ];
    }

    public function prepareData(Iterator $streamEvents): array
    {
        $data = [];

        foreach ($streamEvents as $event) {
            $eventData = $this->messageConverter->convertToArray($event);

            $data[] = $eventData['uuid'];
            $data[] = $eventData['message_name'];
            $data[] = Json::encode($eventData['payload']);
            $data[] = Json::encode($eventData['metadata']);
            $data[] = $eventData['created_at']->format('Y-m-d\TH:i:s.u');
        }

        return $data;
    }

    public function generateTableName(StreamName $streamName): string
    {
        $streamName = $streamName->toString();
        $table = '_' . \sha1($streamName);

        if ($schema = $this->extractSchema($streamName)) {
            $table = $schema . '.' . $table;
        }

        return $table;
    }

    private function getSchemaCreationSchema(string $tableName): array
    {
        if (! $schemaName = $this->extractSchema($tableName)) {
            return [];
        }

        return [\sprintf(
            'CREATE SCHEMA IF NOT EXISTS %s',
            $schemaName
        )];
    }
}
