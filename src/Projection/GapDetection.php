<?php

/**
 * This file is part of prooph/pdo-event-store.
 * (c) 2016-2020 Alexander Miertsch <kontakt@codeliner.ws>
 * (c) 2016-2020 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Prooph\EventStore\Pdo\Projection;

use Prooph\Common\Messaging\Message;

/**
 * Class GapDetection
 *
 * In high load scenarios it is possible that projections skip events due to newer events becoming visible before old ones.
 * For details you can take a look at this issue: https://github.com/prooph/pdo-event-store/issues/189
 *
 * The GapDetection class helps projectors to detect gaps while processing events and coordinates retries to fill gaps.
 *
 * @package Prooph\EventStore\Pdo\Projection
 */
final class GapDetection
{
    /**
     * If gap still exists after all retries, projector moves on
     *
     * You can set your own sleep times. Each number is the sleep time in milliseconds before the next retry is performed
     *
     * @var array
     */
    private $retryConfig = [
        0, // First retry is triggered immediately
        5, // Second retry is triggered after 5ms
        50, // Third retry with much longer sleep time
        500, // Either DB is really busy or we have a real gap, wait another 500ms and run a last try
        // Add more ms values if projection should perform more retries
    ];

    /**
     * There are two reasons for gaps in event streams:
     *
     * 1. A transaction rollback caused a gap
     * 2. Transaction visibility problem described in https://github.com/prooph/pdo-event-store/issues/189
     *
     * The latter can only occur if a projection processes events near realtime.
     * When configured, a detection window ensures that during a projection replay gap retries are not performed. During replays
     * only gaps caused by transaction rollbacks are possible. This avoids unnecessary retries.
     *
     * @var \DateInterval|null
     */
    private $detectionWindow;

    /**
     * @var int
     */
    private $retries = 0;

    public function __construct(array $retryConfig = null, \DateInterval $detectionWindow = null)
    {
        if ($retryConfig) {
            $this->retryConfig = $retryConfig;
        }

        $this->detectionWindow = $detectionWindow;
    }

    public function isRetrying(): bool
    {
        return $this->retries > 0;
    }

    public function trackRetry(): void
    {
        $this->retries++;
    }

    public function resetRetries(): void
    {
        $this->retries = 0;
    }

    public function getSleepForNextRetry(): int
    {
        return (int) $this->retryConfig[$this->retries] ?? 0;
    }

    public function isGapInStreamPosition(int $streamPosition, int $eventPosition): bool
    {
        return $streamPosition + 1 !== $eventPosition;
    }

    public function shouldRetryToFillGap(\DateTimeImmutable $now, Message $currentMessage): bool
    {
        //This check avoids unnecessary retries when replaying projections
        if ($this->detectionWindow && $now->sub($this->detectionWindow) > $currentMessage->createdAt()) {
            return false;
        }

        return \array_key_exists($this->retries, $this->retryConfig);
    }
}
