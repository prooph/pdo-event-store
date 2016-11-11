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

namespace Prooph\EventStore\PDO;

use Iterator;
use PDO;
use PDOStatement;
use Prooph\Common\Messaging\Message;
use Prooph\Common\Messaging\MessageFactory;

final class PDOStreamIterator implements Iterator
{
    /**
     * @var PDO
     */
    private $connection;

    /**
     * @var PDOStatement
     */
    private $statement;

    /**
     * @var array
     */
    private $sql;

    /**
     * @var MessageFactory
     */
    private $messageFactory;

    /**
     * @var array|false
     */
    private $currentItem;

    /**
     * @var int
     */
    private $currentKey;

    /**
     * @var int
     */
    private $batchPosition = 0;

    private $batchSize;

    /**
     * @var int
     */
    private $fromNumber;

    /**
     * @var int|null
     */
    private $count;

    /**
     * @var bool
     */
    private $forward;

    public function __construct(
        PDO $connection,
        MessageFactory $messageFactory,
        array $sql,
        ?int $batchSize,
        int $fromNumber = 0,
        ?int $count,
        bool $forward
    ) {
        if (null === $count) {
            $count = PHP_INT_MAX;
        }

        $this->connection = $connection;
        $this->messageFactory = $messageFactory;
        $this->sql = $sql;
        $this->batchSize = $batchSize;
        $this->fromNumber = $fromNumber;
        $this->count = $count;
        $this->forward = $forward;

        $this->rewind();
    }

    /**
     * @return null|Message
     */
    public function current()
    {
        if (false === $this->currentItem) {
            return;
        }

        $createdAt = \DateTimeImmutable::createFromFormat(
            'Y-m-d\TH:i:s.u',
            $this->currentItem->created_at,
            new \DateTimeZone('UTC')
        );

        return $this->messageFactory->createMessageFromArray($this->currentItem->event_name, [
            'uuid' => $this->currentItem->event_id,
            'created_at' => $createdAt,
            'payload' => json_decode($this->currentItem->payload, true),
            'metadata' => json_decode($this->currentItem->metadata, true)
        ]);
    }

    /**
     * Next
     */
    public function next()
    {
        if (($this->count - 1) === $this->currentKey) {
            $this->currentKey = -1;
            $this->currentItem = false;
            return;
        }

        $this->currentItem = $this->statement->fetch();

        if (false !== $this->currentItem) {
            $this->currentKey++;
        } else {
            $this->batchPosition++;
            if ($this->forward) {
                $limit = $this->batchSize * $this->batchPosition + $this->fromNumber;
            } else {
                $limit = $this->fromNumber - $this->batchSize * $this->batchPosition;
            }
            $this->statement = $this->buildStatement($this->sql, $limit);
            $this->statement->execute();
            $this->statement->setFetchMode(PDO::FETCH_OBJ);

            $this->currentItem = $this->statement->fetch();

            if (false === $this->currentItem) {
                $this->currentKey = -1;
            } else {
                $this->currentKey++;
            }
        }
    }

    /**
     * @return bool|int
     */
    public function key()
    {
        if (-1 === $this->currentKey) {
            return false;
        }

        return $this->currentKey;
    }

    /**
     * @return bool
     */
    public function valid()
    {
        return false !== $this->currentItem;
    }

    /**
     * Rewind
     */
    public function rewind()
    {
        //Only perform rewind if current item is not the first element
        if ($this->currentKey !== 0) {
            $this->batchPosition = 0;
            $this->statement = $this->buildStatement($this->sql, $this->fromNumber);

            $this->statement->execute();
            $this->statement->setFetchMode(PDO::FETCH_OBJ);

            $this->currentItem = null;
            $this->currentKey = -1;

            $this->next();
        }
    }

    private function buildStatement(array $sql, int $fromNumber): PDOStatement
    {
        $limit = $this->count < ($this->batchSize * ($this->batchPosition + 1))
            ? $this->count - ($this->batchSize * $this->batchPosition)
            : $this->batchSize;

        if ($this->forward) {
            $query = $sql['from'] . " WHERE no >= $fromNumber";
        } else {
            $query = $sql['from'] . " WHERE no <= $fromNumber";
        }
        if (isset($sql['where'])) {
            $query .= 'AND ';
            $query .= implode(' AND ', $sql['where']);
        }
        $query .= ' ' . $sql['orderBy'];
        $query .= " LIMIT $limit;";

        return $this->connection->prepare($query);
    }
}
