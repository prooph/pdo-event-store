<?php

declare(strict_types=1);

namespace Prooph\EventStore\Adapter\PDO\JsonQuerier;

use Prooph\EventStore\Adapter\PDO\JsonQuerier;

final class Postgres implements JsonQuerier
{
    public function metadata(string $field): string
    {
        return "metadata->>'$field'";
    }

    public function payload(string $field): string
    {
        return "payload->>'$field'";
    }
}
