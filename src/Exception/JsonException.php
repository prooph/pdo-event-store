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

namespace Prooph\EventStore\Pdo\Exception;

use Prooph\EventStore\Exception\RuntimeException as EventStoreRuntimeException;

class JsonException extends EventStoreRuntimeException implements PdoEventStoreException
{
    /** @deprecated  */
    public static function whileDecode(string $msg, int $code, string $json): JsonException
    {
        return new self(
            \sprintf(
                "Error while decoding JSON. \nMessage: %s \nJSON: %s",
                $msg,
                $json
            ),
            $code
        );
    }
}
