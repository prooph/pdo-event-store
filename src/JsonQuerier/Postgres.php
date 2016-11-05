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
