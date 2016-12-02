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

namespace Prooph\EventStore\PDO\IndexingStrategy;

use Prooph\Common\Messaging\Message;
use Prooph\EventStore\PDO\IndexingStrategy;

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
            "CREATE UNIQUE INDEX  on $tableName ((metadata->>'_aggregate_version'), (metadata->>'_aggregate_id'));",
        ];
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

    public function prepareData(Message $message, array &$data): void
    {
        $data[] = $message->uuid()->toString();
        $data[] = $message->messageName();
        $data[] = json_encode($message->payload());
        $data[] = json_encode($message->metadata());
        $data[] = $message->createdAt()->format('Y-m-d\TH:i:s.u');
    }

    /**
     * @return string[]
     */
    public function uniqueViolationErrorCodes(): array
    {
        return ['23000', '23505'];
    }
}
