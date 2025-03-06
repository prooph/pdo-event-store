<?php

/**
 * This file is part of prooph/pdo-event-store.
 * (c) 2016-2025 Alexander Miertsch <kontakt@codeliner.ws>
 * (c) 2016-2025 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace ProophTest\EventStore\Pdo\Assets\PersistenceStrategy;

use Iterator;
use Prooph\EventStore\Pdo\Exception;
use Prooph\EventStore\Pdo\PersistenceStrategy\MySqlPersistenceStrategy;
use Prooph\EventStore\Pdo\Util\Json;
use Prooph\EventStore\StreamName;

final class CustomMySqlAggregateStreamStrategy implements MySqlPersistenceStrategy
{
    /**
     * @param string $tableName
     * @return string[]
     */
    public function createSchema(string $tableName): array
    {
        $statement = <<<EOT
CREATE TABLE `$tableName` (
    `no` BIGINT(20) NOT NULL AUTO_INCREMENT,
    `event_id` CHAR(36) COLLATE utf8mb4_bin NOT NULL,
    `event_name` VARCHAR(100) COLLATE utf8mb4_bin NOT NULL,
    `payload` JSON NOT NULL,
    `metadata` JSON NOT NULL,
    `created_at` DATETIME(6) NOT NULL,
    `aggregate_version` INT(11) UNSIGNED GENERATED ALWAYS AS (JSON_EXTRACT(metadata, '$._aggregate_version')) STORED NOT NULL UNIQUE KEY,
    PRIMARY KEY (`no`),
    UNIQUE KEY `ix_event_id` (`event_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
EOT;

        return [$statement];
    }

    public function columnNames(): array
    {
        return [
            'no',
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
            if (! isset($event->metadata()['_aggregate_version'])) {
                throw new Exception\RuntimeException('_aggregate_version is missing in metadata');
            }

            $data[] = $event->metadata()['_aggregate_version'];
            $data[] = $event->uuid()->toString();
            $data[] = $event->messageName();
            $data[] = Json::encode($event->payload());
            $data[] = Json::encode($event->metadata());
            $data[] = $event->createdAt()->format('Y-m-d\TH:i:s.u');
        }

        return $data;
    }

    public function generateTableName(StreamName $streamName): string
    {
        return 'events-' . $streamName->toString();
    }
}
