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

namespace Prooph\EventStore\Pdo\Util;

use Prooph\EventStore\Pdo\Exception\JsonException;

class Json
{
    /**
     * @param mixed $value
     *
     * @return string
     *
     * @throws JsonException
     */
    public static function encode($value): string
    {
        $flags = \JSON_UNESCAPED_UNICODE | \JSON_UNESCAPED_SLASHES | \JSON_PRESERVE_ZERO_FRACTION;

        $json = \json_encode($value, $flags);

        if (JSON_ERROR_NONE !== $error = \json_last_error()) {
            throw new JsonException(\json_last_error_msg(), $error);
        }

        return $json;
    }

    /**
     * @param string $json
     *
     * @return mixed
     *
     * @throws JsonException
     */
    public static function decode(string $json)
    {
        $data = \json_decode($json, true, 512, \JSON_BIGINT_AS_STRING);

        if (JSON_ERROR_NONE !== $error = \json_last_error()) {
            throw new JsonException(\json_last_error_msg(), $error);
        }

        return $data;
    }
}
