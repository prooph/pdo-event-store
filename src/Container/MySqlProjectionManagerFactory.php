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

namespace Prooph\EventStore\Pdo\Container;

use Prooph\EventStore\Pdo\Projection\MySqlProjectionManager;

class MySqlProjectionManagerFactory extends AbstractProjectionManagerFactory
{
    protected function projectionManagerClassName(): string
    {
        return MySqlProjectionManager::class;
    }
}
