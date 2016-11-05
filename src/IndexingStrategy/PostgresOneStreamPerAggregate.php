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

final class PostgresOneStreamPerAggregate implements IndexingStrategy
{
    public function createSchema(string $tableName): string
    {
        return <<<EOT
CREATE TABLE $tableName (
    no SERIAL,
    event_id CHAR(36) NOT NULL,
    event_name VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    metadata JSONB NOT NULL,
    created_at CHAR(26) NOT NULL,
    PRIMARY KEY (no),
    UNIQUE (event_id)
);
EOT;
    }

    public function oneStreamPerAggregate(): bool
    {
        return true;
    }

    public function uniqueViolationErrorCode(): string
    {
        return "23505";
    }
}
