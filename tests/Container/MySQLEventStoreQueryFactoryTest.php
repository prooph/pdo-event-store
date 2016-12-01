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
use Prooph\EventStore\PDO\Container\MySQLEventStoreQueryFactory;
use Prooph\EventStore\PDO\Exception\InvalidArgumentException;
use Prooph\EventStore\PDO\IndexingStrategy;
use Prooph\EventStore\PDO\Projection\MySQLEventStoreQuery;
use Prooph\EventStore\PDO\TableNameGeneratorStrategy;
use ProophTest\EventStore\PDO\TestUtil;

final class MySQLEventStoreQueryFactoryTest extends TestCase
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
                    'indexing_strategy' => IndexingStrategy\MySQLSimpleStreamStrategy::class,
                ],
            ],
            'event_store_projection' => [
                'foo' => [
                    'event_store' => 'projection',
                    'connection_service' => 'my_connection',
                ],
            ],
        ];

        $connection = TestUtil::getConnection();

        $container = $this->prophesize(ContainerInterface::class);

        $container->get('my_connection')->willReturn($connection)->shouldBeCalled();
        $container->get('config')->willReturn($config)->shouldBeCalled();
        $container->get(FQCNMessageFactory::class)->willReturn(new FQCNMessageFactory())->shouldBeCalled();
        $container->get(NoOpMessageConverter::class)->willReturn(new NoOpMessageConverter())->shouldBeCalled();
        $container->get(IndexingStrategy\MySQLSimpleStreamStrategy::class)->willReturn(new IndexingStrategy\MySQLSimpleStreamStrategy())->shouldBeCalled();
        $container->get(TableNameGeneratorStrategy\Sha1::class)->willReturn(new TableNameGeneratorStrategy\Sha1())->shouldBeCalled();

        $factory = new MySQLEventStoreQueryFactory('foo');
        $projection = $factory($container->reveal());

        $this->assertInstanceOf(MySQLEventStoreQuery::class, $projection);
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
                    'indexing_strategy' => IndexingStrategy\MySQLSimpleStreamStrategy::class,
                ],
            ],
            'event_store_projection' => [
                'foo' => [
                    'event_store' => 'projection',
                    'connection_options' => TestUtil::getConnectionParams(),
                ],
            ],
        ];

        $connection = TestUtil::getConnection();

        $container = $this->prophesize(ContainerInterface::class);
        $container->get('config')->willReturn($config)->shouldBeCalled();
        $container->get('my_connection')->willReturn($connection)->shouldBeCalled();
        $container->get(FQCNMessageFactory::class)->willReturn(new FQCNMessageFactory())->shouldBeCalled();
        $container->get(NoOpMessageConverter::class)->willReturn(new NoOpMessageConverter())->shouldBeCalled();
        $container->get(IndexingStrategy\MySQLSimpleStreamStrategy::class)->willReturn(new IndexingStrategy\MySQLSimpleStreamStrategy())->shouldBeCalled();
        $container->get(TableNameGeneratorStrategy\Sha1::class)->willReturn(new TableNameGeneratorStrategy\Sha1())->shouldBeCalled();

        $projectionName = 'foo';
        $projection = MySQLEventStoreQueryFactory::$projectionName($container->reveal());

        $this->assertInstanceOf(MySQLEventStoreQuery::class, $projection);
    }

    /**
     * @test
     */
    public function it_throws_exception_when_invalid_container_given(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $projectionName = 'foo';
        MySQLEventStoreQueryFactory::$projectionName('invalid container');
    }
}
