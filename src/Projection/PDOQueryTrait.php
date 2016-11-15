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

use ArrayIterator;
use CachingIterator;
use PDO;
use Prooph\EventStore\Projection\Position;
use Prooph\EventStore\Projection\Query;

trait PDOQueryTrait
{
    /**
     * @var PDO
     */
    protected $connection;

    /**
     * @var string
     */
    protected $eventStreamsTable;

    public function fromCategory(string $name): Query
    {
        $sql = <<<EOT
SELECT real_stream_name FROM $this->eventStreamsTable WHERE real_stream_name LIKE '$name-%';
EOT;
        $statement = $this->connection->prepare($sql);
        $statement->execute();

        $streams = [];
        while ($row = $statement->fetch(PDO::FETCH_OBJ)) {
            $streams[$row->real_stream_name] = 0;
        }

        $this->position = new Position($streams);

        return $this;
    }

    public function fromCategories(string ...$names): Query
    {
        $it = new CachingIterator(new ArrayIterator($names), CachingIterator::FULL_CACHE);

        $where = 'WHERE ';
        foreach ($it as $name) {
            $where .= "real_stream_name LIKE '$name-%'";
            if ($it->hasNext()) {
                $where .= ' OR ';
            }
        }

        $sql = <<<EOT
SELECT real_stream_name FROM $this->eventStreamsTable $where;
EOT;
        $statement = $this->connection->prepare($sql);
        $statement->execute();

        $streams = [];
        while ($row = $statement->fetch(PDO::FETCH_OBJ)) {
            $streams[$row->real_stream_name] = 0;
        }

        $this->position = new Position($streams);

        return $this;
    }

    public function fromAll(): Query
    {
        $sql = <<<EOT
SELECT real_stream_name FROM $this->eventStreamsTable;
EOT;
        $statement = $this->connection->prepare($sql);
        $statement->execute();

        $streams = [];
        while ($row = $statement->fetch(PDO::FETCH_OBJ)) {
            $streams[$row->real_stream_name] = 0;
        }

        $this->position = new Position($streams);

        return $this;
    }
}
