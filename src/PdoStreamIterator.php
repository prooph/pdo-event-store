<?php

/**
 * This file is part of prooph/pdo-event-store.
 * (c) 2016-2019 prooph software GmbH <contact@prooph.de>
 * (c) 2016-2019 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Prooph\EventStore\Pdo;

use DateTimeImmutable;
use DateTimeZone;
use PDO;
use PDOException;
use PDOStatement;
use Prooph\Common\Messaging\Message;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\EventStore\Pdo\Exception\RuntimeException;
use Prooph\EventStore\Pdo\Util\Json;
use Prooph\EventStore\StreamIterator\StreamIterator;

final class PdoStreamIterator implements StreamIterator
{
    /**
     * @var PDOStatement
     */
    private $selectStatement;

    /**
     * @var PDOStatement
     */
    private $countStatement;

    /**
     * @var MessageFactory
     */
    private $messageFactory;

    /**
     * @var \stdClass|false
     */
    private $currentItem = null;

    /**
     * @var int
     */
    private $currentKey = -1;

    /**
     * @var int
     */
    private $batchPosition = 0;

    /**
     * @var int
     */
    private $batchSize;

    /**
     * @var int
     */
    private $fromNumber;

    /**
     * @var int
     */
    private $currentFromNumber;

    /**
     * @var int|null
     */
    private $count;

    /**
     * @var bool
     */
    private $forward;

    public function __construct(
        PDOStatement $selectStatement,
        PDOStatement $countStatement,
        MessageFactory $messageFactory,
        int $batchSize,
        int $fromNumber,
        ?int $count,
        bool $forward
    ) {
        $this->selectStatement = $selectStatement;
        $this->countStatement = $countStatement;
        $this->messageFactory = $messageFactory;
        $this->batchSize = $batchSize;
        $this->fromNumber = $fromNumber;
        $this->currentFromNumber = $fromNumber;
        $this->count = $count;
        $this->forward = $forward;

        $this->next();
    }

    /**
     * @return null|Message
     */
    public function current(): ?Message
    {
        if (false === $this->currentItem) {
            return null;
        }

        $createdAt = $this->currentItem->created_at;

        if (\strlen($createdAt) === 19) {
            $createdAt = $createdAt . '.000';
        }

        $createdAt = DateTimeImmutable::createFromFormat(
            'Y-m-d H:i:s.u',
            $createdAt,
            new DateTimeZone('UTC')
        );

        $metadata = Json::decode($this->currentItem->metadata);

        if (! \array_key_exists('_position', $metadata)) {
            $metadata['_position'] = $this->currentItem->no;
        }

        $payload = Json::decode($this->currentItem->payload);

        return $this->messageFactory->createMessageFromArray($this->currentItem->event_name, [
            'uuid' => $this->currentItem->event_id,
            'created_at' => $createdAt,
            'payload' => $payload,
            'metadata' => $metadata,
        ]);
    }

    public function next(): void
    {
        if ($this->count && ($this->count - 1) === $this->currentKey) {
            $this->currentKey = -1;
            $this->currentItem = false;

            return;
        }

        $this->currentItem = $this->selectStatement->fetch();

        if (false !== $this->currentItem) {
            $this->currentKey++;
            $this->currentFromNumber = $this->currentItem->no;
        } else {
            $this->batchPosition++;
            if ($this->forward) {
                $from = $this->currentFromNumber + 1;
            } else {
                $from = $this->currentFromNumber - 1;
            }
            $this->selectStatement = $this->buildSelectStatement($from);
            try {
                $this->selectStatement->execute();
            } catch (PDOException $exception) {
                // ignore and check error code
            }

            if ($this->selectStatement->errorCode() !== '00000') {
                throw RuntimeException::fromStatementErrorInfo($this->selectStatement->errorInfo());
            }

            $this->selectStatement->setFetchMode(PDO::FETCH_OBJ);

            $this->currentItem = $this->selectStatement->fetch();

            if (false === $this->currentItem) {
                $this->currentKey = -1;
            } else {
                $this->currentKey++;
                $this->currentFromNumber = $this->currentItem->no;
            }
        }
    }

    /**
     * @return bool|int
     */
    public function key()
    {
        if (null === $this->currentItem) {
            return false;
        }

        return $this->currentItem->no;
    }

    /**
     * @return bool
     */
    public function valid(): bool
    {
        return false !== $this->currentItem;
    }

    public function rewind(): void
    {
        //Only perform rewind if current item is not the first element
        if ($this->currentKey !== 0) {
            $this->batchPosition = 0;

            $this->selectStatement = $this->buildSelectStatement($this->fromNumber);
            try {
                $this->selectStatement->execute();
            } catch (PDOException $exception) {
                // ignore and check error code
            }

            if ($this->selectStatement->errorCode() !== '00000') {
                throw RuntimeException::fromStatementErrorInfo($this->selectStatement->errorInfo());
            }

            $this->currentItem = null;
            $this->currentKey = -1;
            $this->currentFromNumber = $this->fromNumber;

            $this->next();
        }
    }

    public function count(): int
    {
        $this->countStatement->bindValue(':fromNumber', $this->fromNumber, PDO::PARAM_INT);

        try {
            if ($this->countStatement->execute()) {
                $count = (int) $this->countStatement->fetchColumn();

                return null === $this->count ? $count : \min($count, $this->count);
            }
        } catch (PDOException $exception) {
            // ignore
        }

        return 0;
    }

    private function buildSelectStatement(int $fromNumber): PDOStatement
    {
        if (null === $this->count
            || $this->count < ($this->batchSize * ($this->batchPosition + 1))
        ) {
            $limit = $this->batchSize;
        } else {
            $limit = $this->count - ($this->batchSize * ($this->batchPosition + 1));
        }

        $this->selectStatement->bindValue(':fromNumber', $fromNumber, PDO::PARAM_INT);
        $this->selectStatement->bindValue(':limit', $limit, PDO::PARAM_INT);

        return $this->selectStatement;
    }
}
