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

use PHPUnit\Framework\TestCase;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\Pdo\Container\MariaDbProjectionManagerFactory;
use Prooph\EventStore\Pdo\Exception\InvalidArgumentException;
use Prooph\EventStore\Pdo\HasQueryHint;
use Prooph\EventStore\Pdo\MariaDbEventStore;
use Prooph\EventStore\Pdo\PersistenceStrategy\MariaDbPersistenceStrategy;
use Prooph\EventStore\Pdo\Projection\MariaDbProjectionManager;
use ProophTest\EventStore\Pdo\TestUtil;
use Prophecy\PhpUnit\ProphecyTrait;
use Psr\Container\ContainerInterface;

/**
 * @group mariadb
 */
class MariaDbProjectionManagerFactoryTest extends TestCase
{
    use ProphecyTrait;

    /**
     * @test
     */
    public function it_creates_service(): void
    {
        $config['prooph']['projection_manager']['default'] = [
            'connection' => 'my_connection',
        ];

        $connection = TestUtil::getConnection();

        $messageFactory = $this->prophesize(MessageFactory::class);
        $persistenceStrategy = $this->prophesize(MariaDbPersistenceStrategy::class);
        $persistenceStrategy->willImplement(HasQueryHint::class);

        $container = $this->prophesize(ContainerInterface::class);
        $eventStore = new MariaDbEventStore(
            $messageFactory->reveal(),
            TestUtil::getConnection(),
            $persistenceStrategy->reveal()
        );

        $container->get('my_connection')->willReturn($connection)->shouldBeCalled();
        $container->get(EventStore::class)->willReturn($eventStore)->shouldBeCalled();
        $container->get('config')->willReturn($config)->shouldBeCalled();

        $factory = new MariaDbProjectionManagerFactory();
        $projectionManager = $factory($container->reveal());

        $this->assertInstanceOf(MariaDbProjectionManager::class, $projectionManager);
    }

    /**
     * @test
     */
    public function it_creates_service_via_callstatic(): void
    {
        $config['prooph']['projection_manager']['default'] = [
            'connection' => 'my_connection',
        ];

        $connection = TestUtil::getConnection();

        $messageFactory = $this->prophesize(MessageFactory::class);
        $persistenceStrategy = $this->prophesize(MariaDbPersistenceStrategy::class);
        $persistenceStrategy->willImplement(HasQueryHint::class);

        $container = $this->prophesize(ContainerInterface::class);
        $eventStore = new MariaDbEventStore(
            $messageFactory->reveal(),
            TestUtil::getConnection(),
            $persistenceStrategy->reveal()
        );

        $container->get('my_connection')->willReturn($connection)->shouldBeCalled();
        $container->get(EventStore::class)->willReturn($eventStore)->shouldBeCalled();
        $container->get('config')->willReturn($config)->shouldBeCalled();

        $name = 'default';
        $pdo = MariaDbProjectionManagerFactory::$name($container->reveal());

        $this->assertInstanceOf(MariaDbProjectionManager::class, $pdo);
    }

    /**
     * @test
     */
    public function it_throws_exception_when_invalid_container_given(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $projectionName = 'custom';
        MariaDbProjectionManagerFactory::$projectionName('invalid container');
    }
}
