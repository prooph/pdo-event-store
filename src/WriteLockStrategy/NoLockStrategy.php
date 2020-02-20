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

namespace Prooph\EventStore\Pdo\WriteLockStrategy;

use Prooph\EventStore\Pdo\WriteLockStrategy;

final class NoLockStrategy implements WriteLockStrategy
{
    public function getLock(string $name): bool
    {
        return true;
    }

    public function releaseLock(string $name): bool
    {
        return true;
    }
}
