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

use Prooph\EventStore\Exception\RuntimeException as EventStoreRuntimeException;

class RuntimeException extends EventStoreRuntimeException implements PdoEventStoreException
{
    public static function fromStatementErrorInfo(array $errorInfo): RuntimeException
    {
        return new self(
            \sprintf(
                "Error %s. \nError-Info: %s",
                $errorInfo[0],
                $errorInfo[2]
            )
        );
    }
}
