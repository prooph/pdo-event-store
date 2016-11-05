<?php

declare(strict_types=1);

namespace Prooph\EventStore\Adapter\PDO;

interface JsonQuerier
{
    public function metadata(string $field): string;

    public function payload(string $field): string;
}
