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

namespace ProophTest\EventStore\Pdo\PersistenceStrategy;

use Prooph\EventStore\Pdo\PersistenceStrategy;
use Prooph\EventStore\Pdo\PersistenceStrategy\PostgresSingleStreamStrategy;

class PostgresSingleStreamStrategyTest extends AbstractPostgresPersistenceStrategyTestCase
{
    protected function createStrategy(): PersistenceStrategy
    {
        return new PostgresSingleStreamStrategy();
    }
}
