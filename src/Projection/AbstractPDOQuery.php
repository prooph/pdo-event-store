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

namespace Prooph\EventStore\PDO\Projection;

use PDO;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\Projection\AbstractQuery;

abstract class AbstractPDOQuery extends AbstractQuery
{
    use PDOQueryTrait;

    public function __construct(EventStore $eventStore, PDO $connection, string $eventStreamsTable)
    {
        parent::__construct($eventStore);

        $this->connection = $connection;
        $this->eventStreamsTable = $eventStreamsTable;
    }
}
