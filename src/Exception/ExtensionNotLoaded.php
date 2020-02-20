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

namespace Prooph\EventStore\Pdo\Exception;

class ExtensionNotLoaded extends RuntimeException
{
    public static function with(string $extensionNme): ExtensionNotLoaded
    {
        return new self(\sprintf('Extension "%s" is not loaded', $extensionNme));
    }
}
