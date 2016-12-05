<?php
/**
 * This file is part of the prooph/pdo-event-store.
 * (c) 2016-2016 prooph software GmbH <contact@prooph.de>
 * (c) 2016-2016 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace ProophTest\EventStore\PDO\Container;

use Interop\Container\ContainerInterface;
use PHPUnit_Framework_TestCase as TestCase;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\PDO\Container\PostgresEventStoreProjectionFactory;
use Prooph\EventStore\PDO\Exception\InvalidArgumentException;
use Prooph\EventStore\PDO\PersistenceStrategy;
use Prooph\EventStore\PDO\Projection\PostgresEventStoreProjection;
use ProophTest\EventStore\PDO\TestUtil;

final class PostgresEventStoreProjectionFactoryTest extends TestCase
{
    /**
     * @test
     */
    public function it_creates_adapter_via_connection_service(): void
    {
        $config['prooph'] = [
            'event_store' => [
                'projection' => [
                    'connection_service' => 'my_connection',
                    'persistence_strategy' => PersistenceStrategy\PostgresSimpleStreamStrategy::class,
                ],
            ],
            'event_store_projection' => [
                'foo' => [
                    'event_store' => 'projection',
                    'connection_service' => 'my_connection',
                    'event_streams_table' => 'event_streams',
                    'projections_table' => 'projection',
                    'lock_timeout_ms' => 1000,
                ],
            ],
        ];

        $connection = TestUtil::getConnection();

        $container = $this->prophesize(ContainerInterface::class);

        $container->get('my_connection')->willReturn($connection)->shouldBeCalled();
        $container->get('config')->willReturn($config)->shouldBeCalled();
        $container->get(FQCNMessageFactory::class)->willReturn(new FQCNMessageFactory())->shouldBeCalled();
        $container->get(NoOpMessageConverter::class)->willReturn(new NoOpMessageConverter())->shouldBeCalled();
        $container->get(PersistenceStrategy\PostgresSimpleStreamStrategy::class)->willReturn(new PersistenceStrategy\PostgresSimpleStreamStrategy())->shouldBeCalled();

        $factory = new PostgresEventStoreProjectionFactory('foo');
        $projection = $factory($container->reveal());

        $this->assertInstanceOf(PostgresEventStoreProjection::class, $projection);
    }

    /**
     * @test
     */
    public function it_creates_adapter_via_connection_options(): void
    {
        $config['prooph'] = [
            'event_store' => [
                'projection' => [
                    'connection_service' => 'my_connection',
                    'persistence_strategy' => PersistenceStrategy\PostgresSimpleStreamStrategy::class,
                ],
            ],
            'event_store_projection' => [
                'foo' => [
                    'event_store' => 'projection',
                    'connection_options' => TestUtil::getConnectionParams(),
                    'event_streams_table' => 'event_streams',
                    'projections_table' => 'projection',
                    'lock_timeout_ms' => 1000,
                ],
            ],
        ];

        $connection = TestUtil::getConnection();

        $container = $this->prophesize(ContainerInterface::class);
        $container->get('config')->willReturn($config)->shouldBeCalled();
        $container->get('my_connection')->willReturn($connection)->shouldBeCalled();
        $container->get(FQCNMessageFactory::class)->willReturn(new FQCNMessageFactory())->shouldBeCalled();
        $container->get(NoOpMessageConverter::class)->willReturn(new NoOpMessageConverter())->shouldBeCalled();
        $container->get(PersistenceStrategy\PostgresSimpleStreamStrategy::class)->willReturn(new PersistenceStrategy\PostgresSimpleStreamStrategy())->shouldBeCalled();

        $projectionName = 'foo';
        $projection = PostgresEventStoreProjectionFactory::$projectionName($container->reveal());

        $this->assertInstanceOf(PostgresEventStoreProjection::class, $projection);
    }

    /**
     * @test
     */
    public function it_throws_exception_when_invalid_container_given(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $projectionName = 'foo';
        PostgresEventStoreProjectionFactory::$projectionName('invalid container');
    }
}
