<?php
/**
 * This file is part of the prooph/pdo-event-store.
 * (c) 2016-2017 prooph software GmbH <contact@prooph.de>
 * (c) 2016-2017 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Prooph\EventStore\PDO\Projection;

use Prooph\EventStore\EventStore;
use Prooph\EventStore\Exception;
use Prooph\EventStore\PDO\Projection\ProjectionOptions as PDOProjectionOptions;
use Prooph\EventStore\Projection\Projection;
use Prooph\EventStore\Projection\ProjectionFactory;
use Prooph\EventStore\Projection\ProjectionOptions;

final class PDOEventStoreProjectionFactory implements ProjectionFactory
{
    public function __invoke(
        EventStore $eventStore,
        string $name,
        ProjectionOptions $options = null
    ): Projection {
        if (! $options instanceof PDOProjectionOptions) {
            throw new Exception\InvalidArgumentException(
                self::class . ' expects an instance of' . PDOProjectionOptions::class
            );
        }

        return new PDOEventStoreProjection(
            $eventStore,
            $options->connection(),
            $name,
            $options->eventStreamsTable(),
            $options->projectionsTable(),
            $options->lockTimeoutMs(),
            $options->cacheSize(),
            $options->persistBlockSize(),
            $options->sleep()
        );
    }
}
