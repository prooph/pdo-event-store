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

namespace ProophTest\EventStore\Pdo\WriteLockStrategy;

use PHPUnit\Framework\TestCase;
use Prooph\EventStore\Pdo\WriteLockStrategy\MysqlMetadataLockStrategy;
use Prophecy\Argument;
use Prophecy\PhpUnit\ProphecyTrait;

/**
 * @group mysql
 */
class MysqlMetadataLockStrategyTest extends TestCase
{
    use ProphecyTrait;

    /**
     * @test
     */
    public function it_returns_true_when_lock_successful()
    {
        $statement = $this->prophesize(\PDOStatement::class);
        $statement->fetchAll()->willReturn([
            0 => ['get_lock' => '1'],
        ]);

        $connection = $this->prophesize(\PDO::class);

        $connection->query(Argument::any())->willReturn($statement->reveal());

        $strategy = new MysqlMetadataLockStrategy($connection->reveal());

        $this->assertTrue($strategy->getLock('lock'));
    }

    /**
     * @test
     */
    public function it_returns_true_when_lock_successful_int()
    {
        $statement = $this->prophesize(\PDOStatement::class);
        $statement->fetchAll()->willReturn([
            0 => ['get_lock' => 1],
        ]);

        $connection = $this->prophesize(\PDO::class);

        $connection->query(Argument::any())->willReturn($statement->reveal());

        $strategy = new MysqlMetadataLockStrategy($connection->reveal());

        $this->assertTrue($strategy->getLock('lock'));
    }

    /**
     * @test
     */
    public function it_requests_lock_with_given_name()
    {
        $statement = $this->prophesize(\PDOStatement::class);
        $statement->fetchAll()->willReturn([
            0 => ['get_lock' => '1'],
        ]);

        $connection = $this->prophesize(\PDO::class);

        $connection->query(Argument::containingString('GET_LOCK(\'lock\''))
            ->willReturn($statement->reveal())
            ->shouldBeCalled();

        $strategy = new MysqlMetadataLockStrategy($connection->reveal());

        $strategy->getLock('lock');
    }

    /**
     * @test
     */
    public function it_requests_lock_without_timeout()
    {
        $statement = $this->prophesize(\PDOStatement::class);
        $statement->fetchAll()->willReturn([
            0 => ['get_lock' => '1'],
        ]);

        $connection = $this->prophesize(\PDO::class);

        $connection->query(Argument::containingString('-1'))
            ->willReturn($statement->reveal())
            ->shouldBeCalled();

        $strategy = new MysqlMetadataLockStrategy($connection->reveal());

        $strategy->getLock('lock');
    }

    /**
     * @test
     */
    public function it_requests_lock_with_configured_timeout()
    {
        $statement = $this->prophesize(\PDOStatement::class);
        $statement->fetchAll()->willReturn([
            0 => ['get_lock' => '1'],
        ]);

        $connection = $this->prophesize(\PDO::class);

        $connection->query(Argument::containingString('100'))
            ->willReturn($statement->reveal())
            ->shouldBeCalled();

        $strategy = new MysqlMetadataLockStrategy($connection->reveal(), 100);

        $strategy->getLock('lock');
    }

    /**
     * @test
     */
    public function it_returns_false_on_statement_error()
    {
        $connection = $this->prophesize(\PDO::class);

        $connection->query(Argument::any())->willReturn(false);

        $strategy = new MysqlMetadataLockStrategy($connection->reveal());

        $this->assertFalse($strategy->getLock('lock'));
    }

    /**
     * @test
     */
    public function it_returns_false_on_lock_failure()
    {
        $statement = $this->prophesize(\PDOStatement::class);
        $statement->fetchAll()->willReturn([
            0 => ['get_lock' => '0'],
        ]);

        $connection = $this->prophesize(\PDO::class);

        $connection->query(Argument::any())->willReturn($statement->reveal());

        $strategy = new MysqlMetadataLockStrategy($connection->reveal());

        $this->assertFalse($strategy->getLock('lock'));
    }

    /**
     * @test
     */
    public function it_returns_false_on_lock_failure_int()
    {
        $statement = $this->prophesize(\PDOStatement::class);
        $statement->fetchAll()->willReturn([
            0 => ['get_lock' => 0],
        ]);

        $connection = $this->prophesize(\PDO::class);

        $connection->query(Argument::any())->willReturn($statement->reveal());

        $strategy = new MysqlMetadataLockStrategy($connection->reveal());

        $this->assertFalse($strategy->getLock('lock'));
    }

    /**
     * @test
     */
    public function it_returns_false_on_lock_killed()
    {
        $statement = $this->prophesize(\PDOStatement::class);
        $statement->fetchAll()->willReturn([
            0 => ['get_lock' => null],
        ]);

        $connection = $this->prophesize(\PDO::class);

        $connection->query(Argument::any())->willReturn($statement->reveal());

        $strategy = new MysqlMetadataLockStrategy($connection->reveal());

        $this->assertFalse($strategy->getLock('lock'));
    }

    /**
     * @test
     */
    public function it_returns_false_on_deadlock_exception()
    {
        $connection = $this->prophesize(\PDO::class);

        $connection->query(Argument::any())->willThrow($this->prophesize(\PDOException::class)->reveal());
        $connection->errorCode()->willReturn('3058');

        $strategy = new MysqlMetadataLockStrategy($connection->reveal());

        $this->assertFalse($strategy->getLock('lock'));
    }

    /**
     * @test
     */
    public function it_releases_lock()
    {
        $connection = $this->prophesize(\PDO::class);

        $connection->exec(Argument::containingString('RELEASE_LOCK(\'lock\''))->shouldBeCalled();

        $strategy = new MysqlMetadataLockStrategy($connection->reveal());

        $strategy->releaseLock('lock');
    }

    /**
     * @test
     */
    public function it_release_returns_true()
    {
        $connection = $this->prophesize(\PDO::class);
        $strategy = new MysqlMetadataLockStrategy($connection->reveal());

        $this->assertTrue($strategy->releaseLock('lock'));
    }
}
