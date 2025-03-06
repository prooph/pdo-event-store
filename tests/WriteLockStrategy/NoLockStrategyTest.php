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

namespace ProophTest\EventStore\Pdo\WriteLockStrategy;

use PHPUnit\Framework\TestCase;
use Prooph\EventStore\Pdo\WriteLockStrategy\NoLockStrategy;
use Prophecy\PhpUnit\ProphecyTrait;

/**
 * @group mysql
 */
class NoLockStrategyTest extends TestCase
{
    use ProphecyTrait;

    /**
     * @test
     */
    public function it_always_succeeds_locking(): void
    {
        $strategy = new NoLockStrategy();

        $this->assertTrue($strategy->getLock('write_lock'));
    }

    /**
     * @test
     */
    public function in_always_succeeds_releasing(): void
    {
        $strategy = new NoLockStrategy();

        $this->assertTrue($strategy->releaseLock('write_lock'));
    }
}
