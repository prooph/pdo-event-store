<?php

/**
 * This file is part of prooph/pdo-event-store.
 * (c) 2016-2022 Alexander Miertsch <kontakt@codeliner.ws>
 * (c) 2016-2022 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Prooph\EventStore\Pdo\Exception;

use Prooph\EventStore\Exception\ConcurrencyException;

class ConcurrencyExceptionFactory
{
    public static function fromStatementErrorInfo(array $errorInfo): ConcurrencyException
    {
        return new ConcurrencyException(
            \sprintf(
                "Some of the aggregate IDs or event IDs have already been used in the same stream. The PDO error should contain more information:\nError %s.\nError-Info: %s",
                $errorInfo[0],
                $errorInfo[2]
            )
        );
    }

    public static function failedToAcquireLock(): ConcurrencyException
    {
        return new ConcurrencyException('Failed to acquire lock for writing to stream');
    }
}
