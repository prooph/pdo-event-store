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

namespace Prooph\EventStore\Adapter\PDO\IndexingStrategy;

use Prooph\EventStore\Adapter\PDO\IndexingStrategy;

final class PostgresSingleStreamStrategy implements IndexingStrategy
{
    /**
     * @param string $tableName
     * @return string[]
     */
    public function createSchema(string $tableName): array
    {
        $statement = <<<EOT
CREATE TABLE $tableName (
    no SERIAL,
    event_id CHAR(36) NOT NULL,
    event_name VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    metadata JSONB NOT NULL,
    created_at CHAR(26) NOT NULL,
    PRIMARY KEY (no),
    CONSTRAINT aggregate_version_not_null CHECK ((metadata->>'_aggregate_version') IS NOT NULL),
    CONSTRAINT aggregate_type_not_null CHECK ((metadata->>'_aggregate_type') IS NOT NULL),
    CONSTRAINT aggregate_id_not_null CHECK ((metadata->>'_aggregate_id') IS NOT NULL),
    UNIQUE (event_id)
);
EOT;
        return [
            $statement,
            "CREATE UNIQUE INDEX  on $tableName ((metadata->>'_aggregate_version'), (metadata->>'_aggregate_id'));"
        ];
    }

    public function oneStreamPerAggregate(): bool
    {
        return false;
    }

    /**
     * @return string[]
     */
    public function uniqueViolationErrorCodes(): array
    {
        return ["23000", "23505"];
    }
}
