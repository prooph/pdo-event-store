<?php
/**
 * This file is part of the prooph/event-store-mysql-adapter.
 * (c) 2016-2016 prooph software GmbH <contact@prooph.de>
 * (c) 2016-2016 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Prooph\EventStore\Adapter\MySQL;

use Assert\Assertion;
use Iterator;
use Prooph\Common\Messaging\Message;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\EventStore\Adapter\PayloadSerializer;

final class MySQLStreamIterator implements Iterator
{
    /**
     * @var QueryBuilder
     */
    private $queryBuilder;

    /**
     * @var PDOStatement
     */
    private $statement;

    /**
     * @var MessageFactory
     */
    private $messageFactory;

    /**
     * @var PayloadSerializer
     */
    private $payloadSerializer;

    /**
     * @var array
     */
    private $metadata;

    /**
     * @var array
     */
    private $standardColumns = ['event_id', 'event_name', 'created_at', 'payload', 'version'];

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
     * @param QueryBuilder $queryBuilder
     * @param MessageFactory $messageFactory
     * @param PayloadSerializer $payloadSerializer
     * @param array $metadata
     * @param int $batchSize
     */
    public function __construct(
        QueryBuilder $queryBuilder,
        MessageFactory $messageFactory,
        PayloadSerializer $payloadSerializer,
        array $metadata,
        $batchSize = 10000
    ) {
        Assertion::integer($batchSize);

        $this->queryBuilder = $queryBuilder;
        $this->messageFactory = $messageFactory;
        $this->payloadSerializer = $payloadSerializer;
        $this->metadata = $metadata;
        $this->batchSize = $batchSize;

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

        $payload = $this->payloadSerializer->unserializePayload($this->currentItem['payload']);

        $metadata = [];

        //Add metadata stored in table
        foreach ($this->currentItem as $key => $value) {
            if (! in_array($key, $this->standardColumns)) {
                $metadata[$key] = $value;
            }
        }

        $createdAt = \DateTimeImmutable::createFromFormat(
            'Y-m-d\TH:i:s.u',
            $this->currentItem['created_at'],
            new \DateTimeZone('UTC')
        );

        return $this->messageFactory->createMessageFromArray($this->currentItem['event_name'], [
            'uuid' => $this->currentItem['event_id'],
            'version' => (int) $this->currentItem['version'],
            'created_at' => $createdAt,
            'payload' => $payload,
            'metadata' => $metadata
        ]);
    }

    /**
     * Next
     */
    public function next()
    {
        $this->currentItem = $this->statement->fetch();

        if (false !== $this->currentItem) {
            $this->currentKey++;
        } else {
            $this->batchPosition++;
            $this->queryBuilder->setFirstResult($this->batchSize * $this->batchPosition);
            $this->queryBuilder->setMaxResults($this->batchSize);
            /* @var $stmt \Doctrine\DBAL\Statement */
            $this->statement = $this->queryBuilder->execute();
            $this->statement->setFetchMode(\PDO::FETCH_ASSOC);

            $this->currentItem = $this->statement->fetch();

            if (false === $this->currentItem) {
                $this->currentKey = -1;
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
            $this->queryBuilder->setFirstResult(0);
            $this->queryBuilder->setMaxResults($this->batchSize);

            /* @var $stmt \Doctrine\DBAL\Statement */
            $stmt = $this->queryBuilder->execute();
            $stmt->setFetchMode(\PDO::FETCH_ASSOC);

            $this->currentItem = null;
            $this->currentKey = -1;
            $this->statement = $stmt;

            $this->next();
        }
    }
}
