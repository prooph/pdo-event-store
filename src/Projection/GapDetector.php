<?php

/**
 * This file is part of prooph/pdo-event-store.
 * (c) 2016-2025 Alexander Miertsch <kontakt@codeliner.ws>
 * (c) 2016-2025 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Prooph\EventStore\Pdo\Projection;

use DateTimeImmutable;
use Prooph\Common\Messaging\Message;

/**
 * @see GapDetection for a default implemenation
 */
interface GapDetector
{
    public function isRetrying(): bool;

    public function trackRetry(): void;

    public function resetRetries(): void;

    public function getSleepForNextRetry(): int;

    public function isGapInStreamPosition(int $streamPosition, int $eventPosition): bool;

    public function shouldRetryToFillGap(DateTimeImmutable $now, Message $currentMessage): bool;
}
