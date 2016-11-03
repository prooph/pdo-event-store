<?php
/**
 * This file is part of the prooph/event-store-pdo-adapter.
 * (c) 2016-2016 prooph software GmbH <contact@prooph.de>
 * (c) 2016-2016 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Prooph\EventStore\Adapter\PDO;

final class MySQLMultipleStreamsPerAggregateIndexingStrategy implements IndexingStrategy
{
    public function __invoke(string $tableName): string
    {
        return <<<EOT
CREATE TABLE `$tableName` (
    `no` INT(11) NOT NULL AUTO_INCREMENT,
    `event_id` CHAR(36) COLLATE utf8_unicode_ci NOT NULL,
    `event_name` VARCHAR(100) COLLATE utf8_unicode_ci NOT NULL,
    `payload` JSON NOT NULL,
    `metadata` JSON NOT NULL,
    `created_at` CHAR(26) COLLATE utf8_unicode_ci NOT NULL,
    `version` INT GENERATED ALWAYS AS (JSON_EXTRACT(metadata, '$._version')),
    `aggregate_id` char(36) GENERATED ALWAYS AS (JSON_EXTRACT(metadata, '$._aggregate_id')),
    `aggregate_type` varchar(150) GENERATED ALWAYS AS (JSON_EXTRACT(metadata, '$._aggregate_type')),
    PRIMARY KEY (`no`),
    UNIQUE KEY `ix_event_id` (`event_id`),
    UNIQUE KEY `ix_unique_event` (`version`, `aggregate_id`),
    UNIQUE KEY `ix_aggregate_type` (`aggregate_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
EOT;
    }
}
