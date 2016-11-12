<?php
/**
 * This file is part of the prooph/pdo-event-store.
 * (c) 2016-2016 prooph software GmbH <contact@prooph.de>
 * (c) 2016-2016 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace ProophTest\EventStore\PDO\Container;

use Interop\Container\ContainerInterface;
use PHPUnit_Framework_TestCase as TestCase;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\PDO\Container\PostgresEventStoreFactory;
use Prooph\EventStore\PDO\IndexingStrategy;
use Prooph\EventStore\PDO\PostgresEventStore;
use Prooph\EventStore\PDO\TableNameGeneratorStrategy;
use ProophTest\EventStore\PDO\TestUtil;

final class PostgresEventStoreFactoryTest extends TestCase
{
    /**
     * @test
     */
    public function it_creates_adapter_via_connection_service(): void
    {
        $config['prooph']['event_store']['default'] = [
            'connection_service' => 'my_connection',
            'indexing_strategy' => IndexingStrategy\MySQLAggregateStreamStrategy::class,
        ];

        $connection = TestUtil::getConnection();

        $container = $this->prophesize(ContainerInterface::class);

        $container->get('my_connection')->willReturn($connection)->shouldBeCalled();
        $container->get('config')->willReturn($config)->shouldBeCalled();
        $container->get(FQCNMessageFactory::class)->willReturn(new FQCNMessageFactory())->shouldBeCalled();
        $container->get(NoOpMessageConverter::class)->willReturn(new NoOpMessageConverter())->shouldBeCalled();
        $container->get(IndexingStrategy\MySQLAggregateStreamStrategy::class)->willReturn(new IndexingStrategy\MySQLAggregateStreamStrategy())->shouldBeCalled();
        $container->get(TableNameGeneratorStrategy\Sha1::class)->willReturn(new TableNameGeneratorStrategy\Sha1())->shouldBeCalled();

        $factory = new PostgresEventStoreFactory();
        $adapter = $factory($container->reveal());

        $this->assertInstanceOf(PostgresEventStore::class, $adapter);
    }

    /**
     * @test
     */
    public function it_creates_adapter_via_connection_options(): void
    {
        $config['prooph']['event_store']['custom'] = [
            'connection_options' => TestUtil::getConnectionParams(),
            'indexing_strategy' => IndexingStrategy\MySQLAggregateStreamStrategy::class,
        ];

        $container = $this->prophesize(ContainerInterface::class);

        $container->get('config')->willReturn($config)->shouldBeCalled();
        $container->get(FQCNMessageFactory::class)->willReturn(new FQCNMessageFactory())->shouldBeCalled();
        $container->get(NoOpMessageConverter::class)->willReturn(new NoOpMessageConverter())->shouldBeCalled();
        $container->get(IndexingStrategy\MySQLAggregateStreamStrategy::class)->willReturn(new IndexingStrategy\MySQLAggregateStreamStrategy())->shouldBeCalled();
        $container->get(TableNameGeneratorStrategy\Sha1::class)->willReturn(new TableNameGeneratorStrategy\Sha1())->shouldBeCalled();

        $eventStoreName = 'custom';
        $adapter = PostgresEventStoreFactory::$eventStoreName($container->reveal());

        $this->assertInstanceOf(PostgresEventStore::class, $adapter);
    }
}
