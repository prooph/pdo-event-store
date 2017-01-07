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
use Prooph\EventStore\Projection\ProjectionOptions;
use Prooph\EventStore\Projection\Query;
use Prooph\EventStore\Projection\QueryFactory;

final class PDOEventStoreQueryFactory implements QueryFactory
{
    public function __invoke(EventStore $eventStore, ProjectionOptions $options = null): Query
    {
        if (null === $options) {
            $options = new PDOProjectionOptions();
        }

        if (! $options instanceof PDOProjectionOptions) {
            throw new Exception\InvalidArgumentException(
                self::class . ' expects an instance of' . PDOProjectionOptions::class
            );
        }

        return new PDOEventStoreQuery($eventStore, $options->connection(), $options->eventStreamsTable());
    }
}
