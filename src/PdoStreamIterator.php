<?php
/**
 * This file is part of the prooph/pdo-event-store.
 * (c) 2016-2018 prooph software GmbH <contact@prooph.de>
 * (c) 2016-2018 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Prooph\EventStore\Pdo;

use DateTimeImmutable;
use DateTimeZone;
use Iterator;
use PDO;
use PDOException;
use PDOStatement;
use Prooph\Common\Messaging\Message;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\EventStore\Pdo\Exception\JsonException;
use Prooph\EventStore\Pdo\Exception\RuntimeException;

final class PdoStreamIterator implements Iterator
{
    /**
     * @var PDOStatement
     */
    private $statement;

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
        PDOStatement $statement,
        MessageFactory $messageFactory,
        int $batchSize,
        int $fromNumber,
        ?int $count,
        bool $forward
    ) {
        $this->statement = $statement;
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

        $metadata = \json_decode($this->currentItem->metadata, true);

        if (! \array_key_exists('_position', $metadata)) {
            $metadata['_position'] = $this->currentItem->no;
        }

        $payload = \json_decode($this->currentItem->payload, true);
        if (\json_last_error() !== JSON_ERROR_NONE) {
            throw JsonException::whileDecode(
                \json_last_error_msg(),
                \json_last_error(),
                $this->currentItem->payload
            );
        }

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

        $this->currentItem = $this->statement->fetch();

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
            $this->statement = $this->buildStatement($from);
            try {
                $this->statement->execute();
            } catch (PDOException $exception) {
                // ignore and check error code
            }

            if ($this->statement->errorCode() !== '00000') {
                throw RuntimeException::fromStatementErrorInfo($this->statement->errorInfo());
            }

            $this->statement->setFetchMode(PDO::FETCH_OBJ);

            $this->currentItem = $this->statement->fetch();

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

            $this->statement = $this->buildStatement($this->fromNumber);
            try {
                $this->statement->execute();
            } catch (PDOException $exception) {
                // ignore and check error code
            }

            if ($this->statement->errorCode() !== '00000') {
                throw RuntimeException::fromStatementErrorInfo($this->statement->errorInfo());
            }

            $this->currentItem = null;
            $this->currentKey = -1;
            $this->currentFromNumber = $this->fromNumber;

            $this->next();
        }
    }

    private function buildStatement(int $fromNumber): PDOStatement
    {
        if (null === $this->count
            || $this->count < ($this->batchSize * ($this->batchPosition + 1))
        ) {
            $limit = $this->batchSize;
        } else {
            $limit = $this->count - ($this->batchSize * ($this->batchPosition + 1));
        }

        $this->statement->bindValue(':fromNumber', $fromNumber, PDO::PARAM_INT);
        $this->statement->bindValue(':limit', $limit, PDO::PARAM_INT);

        return $this->statement;
    }
}
