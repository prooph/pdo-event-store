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
use Prooph\EventStore\Pdo\WriteLockStrategy\PostgresAdvisoryLockStrategy;
use Prophecy\Argument;
use Prophecy\PhpUnit\ProphecyTrait;

/**
 * @group postgres
 */
class PostgresAdvisoryLockStrategyTest extends TestCase
{
    use ProphecyTrait;

    /**
     * @test
     */
    public function it_returns_true_when_lock_successful(): void
    {
        $connection = $this->prophesize(\PDO::class);

        $connection->exec(Argument::any())->willReturn(0);

        $strategy = new PostgresAdvisoryLockStrategy($connection->reveal());

        $this->assertTrue($strategy->getLock('lock'));
    }

    /**
     * @test
     */
    public function it_requests_lock_with_given_name(): void
    {
        $connection = $this->prophesize(\PDO::class);

        $connection->exec(Argument::containingString('pg_advisory_lock'))
            ->willReturn(0)
            ->shouldBeCalled();

        $strategy = new PostgresAdvisoryLockStrategy($connection->reveal());

        $strategy->getLock('lock');
    }

    /**
     * @test
     */
    public function it_releases_lock(): void
    {
        $connection = $this->prophesize(\PDO::class);

        $connection->exec(Argument::containingString('pg_advisory_unlock'))
            ->shouldBeCalled()
            ->willReturn(0);

        $strategy = new PostgresAdvisoryLockStrategy($connection->reveal());

        $strategy->releaseLock('lock');
    }

    /**
     * @test
     */
    public function it_release_returns_true(): void
    {
        $connection = $this->prophesize(\PDO::class);

        $connection->exec(Argument::any())
            ->willReturn(0);

        $strategy = new PostgresAdvisoryLockStrategy($connection->reveal());

        $this->assertTrue($strategy->releaseLock('lock'));
    }
}
