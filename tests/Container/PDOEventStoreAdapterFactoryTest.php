<?php
/**
 * This file is part of the prooph/event-store-pdo-adapter.
 * (c) 2016-2016 prooph software GmbH <contact@prooph.de>
 * (c) 2016-2016 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace ProophTest\EventStore\Adapter\PDO\Container;

use Interop\Container\ContainerInterface;
use PHPUnit_Framework_TestCase as TestCase;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\MessageConverter;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\Adapter\PDO\IndexingStrategy;
use Prooph\EventStore\Adapter\PDO\PDOEventStoreAdapter;
use Prooph\EventStore\Adapter\PDO\Container\PDOEventStoreAdapterFactory;
use Prooph\EventStore\Adapter\PDO\TableNameGeneratorStrategy;
use ProophTest\EventStore\Adapter\PDO\TestUtil;

final class PDOEventStoreAdapterFactoryTest extends TestCase
{
    /**
     * @test
     */
    public function it_creates_adapter_via_connection_service(): void
    {
        $config['prooph']['event_store']['default']['adapter']['options'] = [
            'connection_service' => 'my_connection',
        ];

        $connection = TestUtil::getConnection();

        $container = $this->prophesize(ContainerInterface::class);

        $container->get('my_connection')->willReturn($connection)->shouldBeCalled();
        $container->get('config')->willReturn($config)->shouldBeCalled();
        $container->has(MessageFactory::class)->willReturn(false)->shouldBeCalled();
        $container->has(MessageConverter::class)->willReturn(false)->shouldBeCalled();
        $container->get(IndexingStrategy\MySQLOneStreamPerAggregate::class)->willReturn(new IndexingStrategy\MySQLOneStreamPerAggregate())->shouldBeCalled();
        $container->get(TableNameGeneratorStrategy\Sha1::class)->willReturn(new TableNameGeneratorStrategy\Sha1())->shouldBeCalled();

        $factory = new PDOEventStoreAdapterFactory();
        $adapter = $factory($container->reveal());

        $this->assertInstanceOf(PDOEventStoreAdapter::class, $adapter);
    }

    /**
     * @test
     */
    public function it_creates_adapter_via_connection_options(): void
    {
        $connection = TestUtil::getConnection();
        $connection->exec('CREATE DATABASE ' . TestUtil::getDatabaseName());
        $config['prooph']['event_store']['custom']['adapter']['options'] = [
            'connection_options' => TestUtil::getConnectionParams(),
        ];

        $container = $this->prophesize(ContainerInterface::class);

        $container->get('config')->willReturn($config)->shouldBeCalled();
        $container->has(MessageFactory::class)->willReturn(true)->shouldBeCalled();
        $container->get(MessageFactory::class)->willReturn(new FQCNMessageFactory())->shouldBeCalled();
        $container->has(MessageConverter::class)->willReturn(true)->shouldBeCalled();
        $container->get(MessageConverter::class)->willReturn(new NoOpMessageConverter())->shouldBeCalled();
        $container->get(IndexingStrategy\MySQLOneStreamPerAggregate::class)->willReturn(new IndexingStrategy\MySQLOneStreamPerAggregate())->shouldBeCalled();
        $container->get(TableNameGeneratorStrategy\Sha1::class)->willReturn(new TableNameGeneratorStrategy\Sha1())->shouldBeCalled();

        $eventStoreName = 'custom';
        $adapter = PDOEventStoreAdapterFactory::$eventStoreName($container->reveal());

        $this->assertInstanceOf(PDOEventStoreAdapter::class, $adapter);
        $connection->exec('DROP DATABASE ' . TestUtil::getDatabaseName());
    }
}
