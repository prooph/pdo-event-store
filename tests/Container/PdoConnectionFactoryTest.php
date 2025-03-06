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

namespace ProophTest\EventStore\Pdo\Container;

use PDO;
use PHPUnit\Framework\TestCase;
use Prooph\EventStore\Pdo\Container\PdoConnectionFactory;
use Prooph\EventStore\Pdo\Exception\InvalidArgumentException;
use ProophTest\EventStore\Pdo\TestUtil;
use Prophecy\PhpUnit\ProphecyTrait;
use Psr\Container\ContainerInterface;

class PdoConnectionFactoryTest extends TestCase
{
    use ProphecyTrait;

    /**
     * @var array
     */
    protected $config;

    protected function setUp(): void
    {
        $vendor = TestUtil::getDatabaseDriver();

        if ($vendor === 'pdo_mysql') {
            $vendor = 'mysql';
        } elseif ($vendor === 'pdo_pgsql') {
            $vendor = 'pgsql';
        } else {
            throw new \RuntimeException('Invalid database vendor');
        }

        $this->config = [
            'prooph' => [
                'pdo_connection' => [
                    'default' => \array_merge(TestUtil::getConnectionParams(), ['schema' => $vendor]),
                ],
            ],
        ];
    }

    /**
     * @test
     * @group mysql
     */
    public function it_creates_mysql_connection(): void
    {
        $container = $this->prophesize(ContainerInterface::class);

        $container->get('config')->willReturn($this->config)->shouldBeCalled();

        $factory = new PdoConnectionFactory();
        $pdo = $factory($container->reveal());

        $this->assertInstanceOf(PDO::class, $pdo);
    }

    /**
     * @test
     * @group mysql
     */
    public function it_creates_mysql_connection_via_callstatic(): void
    {
        $container = $this->prophesize(ContainerInterface::class);

        $container->get('config')->willReturn($this->config)->shouldBeCalled();

        $name = 'default';
        $pdo = PdoConnectionFactory::$name($container->reveal());

        $this->assertInstanceOf(PDO::class, $pdo);
    }

    /**
     * @test
     * @group postgres
     */
    public function it_creates_postgres_connection(): void
    {
        $container = $this->prophesize(ContainerInterface::class);

        $container->get('config')->willReturn($this->config)->shouldBeCalled();

        $factory = new PdoConnectionFactory();
        $pdo = $factory($container->reveal());

        $this->assertInstanceOf(PDO::class, $pdo);
    }

    /**
     * @test
     * @group postgres
     */
    public function it_creates_postgres_connection_via_callstatic(): void
    {
        $container = $this->prophesize(ContainerInterface::class);

        $container->get('config')->willReturn($this->config)->shouldBeCalled();

        $name = 'default';
        $pdo = PdoConnectionFactory::$name($container->reveal());

        $this->assertInstanceOf(PDO::class, $pdo);
    }

    /**
     * @test
     */
    public function it_throws_exception_when_invalid_container_given(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $projectionName = 'custom';
        PdoConnectionFactory::$projectionName('invalid container');
    }
}
