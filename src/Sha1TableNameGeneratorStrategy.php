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

use Prooph\EventStore\Stream\StreamName;

final class Sha1TableNameGeneratorStrategy implements TableNameGeneratorStrategy
{
    public function __invoke(StreamName $streamName): string
    {
        return sha1($streamName->toString());
    }
}
