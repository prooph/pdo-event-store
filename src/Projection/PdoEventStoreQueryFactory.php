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

namespace Prooph\EventStore\Pdo\Projection;

use PDO;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\Projection\Query;
use Prooph\EventStore\Projection\QueryFactory;

final class PdoEventStoreQueryFactory implements QueryFactory
{
    /**
     * @var PDO
     */
    private $connection;

    /**
     * @var string
     */
    private $eventStreamsTable;

    public function __construct(PDO $connection, string $eventStreamsTable)
    {
        $this->connection = $connection;
        $this->eventStreamsTable = $eventStreamsTable;
    }

    public function __invoke(EventStore $eventStore): Query
    {
        return new PdoEventStoreQuery($eventStore, $this->connection, $this->eventStreamsTable);
    }
}
