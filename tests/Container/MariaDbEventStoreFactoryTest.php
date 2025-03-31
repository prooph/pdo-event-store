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
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\EventStore\ActionEventEmitterEventStore;
use Prooph\EventStore\Exception\ConfigurationException;
use Prooph\EventStore\Metadata\MetadataEnricher;
use Prooph\EventStore\Pdo\Container\MariaDbEventStoreFactory;
use Prooph\EventStore\Pdo\Exception\InvalidArgumentException;
use Prooph\EventStore\Pdo\MariaDbEventStore;
use Prooph\EventStore\Pdo\PersistenceStrategy;
use Prooph\EventStore\Pdo\WriteLockStrategy;
use Prooph\EventStore\Pdo\WriteLockStrategy\MariaDbMetadataLockStrategy;
use Prooph\EventStore\Plugin\Plugin;
use ProophTest\EventStore\Pdo\TestUtil;
use Prophecy\PhpUnit\ProphecyTrait;
use Psr\Container\ContainerInterface;

/**
 * @group mariadb
 */
final class MariaDbEventStoreFactoryTest extends TestCase
{
    use ProphecyTrait;

    /**
     * @test
     */
    public function it_creates_adapter_via_connection_service(): void
    {
        $config['prooph']['event_store']['default'] = [
            'connection' => 'my_connection',
            'persistence_strategy' => PersistenceStrategy\MariaDbAggregateStreamStrategy::class,
            'wrap_action_event_emitter' => false,
        ];

        $connection = TestUtil::getConnection();

        $container = $this->prophesize(ContainerInterface::class);

        $container->get('my_connection')->willReturn($connection)->shouldBeCalled();
        $container->get('config')->willReturn($config)->shouldBeCalled();
        $container->get(FQCNMessageFactory::class)->willReturn(new FQCNMessageFactory())->shouldBeCalled();
        $container->get(PersistenceStrategy\MariaDbAggregateStreamStrategy::class)->willReturn($this->prophesize(PersistenceStrategy\MariaDbPersistenceStrategy::class))->shouldBeCalled();

        $factory = new MariaDbEventStoreFactory();
        $eventStore = $factory($container->reveal());

        $this->assertInstanceOf(MariaDbEventStore::class, $eventStore);
    }

    /**
     * @test
     */
    public function it_wraps_action_event_emitter(): void
    {
        $config['prooph']['event_store']['custom'] = [
            'connection' => 'my_connection',
            'persistence_strategy' => PersistenceStrategy\MariaDbAggregateStreamStrategy::class,
        ];

        $connection = TestUtil::getConnection();

        $container = $this->prophesize(ContainerInterface::class);

        $container->get('config')->willReturn($config)->shouldBeCalled();
        $container->get('my_connection')->willReturn($connection)->shouldBeCalled();
        $container->get(FQCNMessageFactory::class)->willReturn(new FQCNMessageFactory())->shouldBeCalled();
        $container->get(PersistenceStrategy\MariaDbAggregateStreamStrategy::class)->willReturn($this->prophesize(PersistenceStrategy\MariaDbPersistenceStrategy::class))->shouldBeCalled();

        $eventStoreName = 'custom';
        $eventStore = MariaDbEventStoreFactory::$eventStoreName($container->reveal());

        $this->assertInstanceOf(ActionEventEmitterEventStore::class, $eventStore);
    }

    /**
     * @test
     */
    public function it_injects_plugins(): void
    {
        $config['prooph']['event_store']['custom'] = [
            'connection' => 'my_connection',
            'persistence_strategy' => PersistenceStrategy\MariaDbAggregateStreamStrategy::class,
            'plugins' => ['plugin'],
        ];

        $connection = TestUtil::getConnection();

        $container = $this->prophesize(ContainerInterface::class);

        $container->get('config')->willReturn($config)->shouldBeCalled();
        $container->get('my_connection')->willReturn($connection)->shouldBeCalled();
        $container->get(FQCNMessageFactory::class)->willReturn(new FQCNMessageFactory())->shouldBeCalled();
        $container->get(PersistenceStrategy\MariaDbAggregateStreamStrategy::class)->willReturn($this->prophesize(PersistenceStrategy\MariaDbPersistenceStrategy::class))->shouldBeCalled();

        $featureMock = $this->getMockForAbstractClass(Plugin::class);
        $featureMock->expects($this->once())->method('attachToEventStore');

        $container->get('plugin')->willReturn($featureMock);

        $eventStoreName = 'custom';
        $eventStore = MariaDbEventStoreFactory::$eventStoreName($container->reveal());

        $this->assertInstanceOf(ActionEventEmitterEventStore::class, $eventStore);
    }

    /**
     * @test
     */
    public function it_throws_exception_when_invalid_plugin_configured(): void
    {
        $this->expectException(ConfigurationException::class);
        $this->expectExceptionMessage('Plugin plugin does not implement the Plugin interface');

        $config['prooph']['event_store']['custom'] = [
            'connection' => 'my_connection',
            'persistence_strategy' => PersistenceStrategy\MariaDbAggregateStreamStrategy::class,
            'plugins' => ['plugin'],
        ];

        $connection = TestUtil::getConnection();

        $container = $this->prophesize(ContainerInterface::class);

        $container->get('config')->willReturn($config)->shouldBeCalled();
        $container->get('my_connection')->willReturn($connection)->shouldBeCalled();
        $container->get(FQCNMessageFactory::class)->willReturn(new FQCNMessageFactory())->shouldBeCalled();
        $container->get(PersistenceStrategy\MariaDbAggregateStreamStrategy::class)->willReturn($this->prophesize(PersistenceStrategy\MariaDbPersistenceStrategy::class))->shouldBeCalled();

        $container->get('plugin')->willReturn('notAValidPlugin');

        $eventStoreName = 'custom';
        MariaDbEventStoreFactory::$eventStoreName($container->reveal());
    }

    /**
     * @test
     */
    public function it_injects_metadata_enrichers(): void
    {
        $config['prooph']['event_store']['custom'] = [
            'connection' => 'my_connection',
            'persistence_strategy' => PersistenceStrategy\MariaDbAggregateStreamStrategy::class,
            'metadata_enrichers' => ['metadata_enricher1', 'metadata_enricher2'],
        ];

        $metadataEnricher1 = $this->prophesize(MetadataEnricher::class);
        $metadataEnricher2 = $this->prophesize(MetadataEnricher::class);

        $connection = TestUtil::getConnection();

        $container = $this->prophesize(ContainerInterface::class);

        $container->get('config')->willReturn($config);
        $container->get('my_connection')->willReturn($connection)->shouldBeCalled();
        $container->get(FQCNMessageFactory::class)->willReturn(new FQCNMessageFactory())->shouldBeCalled();
        $container->get(PersistenceStrategy\MariaDbAggregateStreamStrategy::class)->willReturn($this->prophesize(PersistenceStrategy\MariaDbPersistenceStrategy::class))->shouldBeCalled();

        $container->get('metadata_enricher1')->willReturn($metadataEnricher1->reveal());
        $container->get('metadata_enricher2')->willReturn($metadataEnricher2->reveal());

        $eventStoreName = 'custom';
        $eventStore = MariaDbEventStoreFactory::$eventStoreName($container->reveal());

        $this->assertInstanceOf(ActionEventEmitterEventStore::class, $eventStore);
    }

    /**
     * @test
     */
    public function it_throws_exception_when_invalid_metadata_enricher_configured(): void
    {
        $this->expectException(ConfigurationException::class);
        $this->expectExceptionMessage('Metadata enricher foobar does not implement the MetadataEnricher interface');

        $config['prooph']['event_store']['custom'] = [
            'connection' => 'my_connection',
            'persistence_strategy' => PersistenceStrategy\MariaDbAggregateStreamStrategy::class,
            'metadata_enrichers' => ['foobar'],
        ];

        $connection = TestUtil::getConnection();

        $container = $this->prophesize(ContainerInterface::class);

        $container->get('config')->willReturn($config);
        $container->get('my_connection')->willReturn($connection)->shouldBeCalled();
        $container->get(FQCNMessageFactory::class)->willReturn(new FQCNMessageFactory())->shouldBeCalled();
        $container->get(PersistenceStrategy\MariaDbAggregateStreamStrategy::class)->willReturn($this->prophesize(PersistenceStrategy\MariaDbPersistenceStrategy::class))->shouldBeCalled();

        $container->get('foobar')->willReturn('foobar');

        $eventStoreName = 'custom';
        MariaDbEventStoreFactory::$eventStoreName($container->reveal());
    }

    /**
     * @test
     */
    public function it_loads_write_lock_if_set(): void
    {
        $config['prooph']['event_store']['default'] = [
            'connection' => 'my_connection',
            'persistence_strategy' => PersistenceStrategy\MySqlAggregateStreamStrategy::class,
            'write_lock_strategy' => MariaDbMetadataLockStrategy::class,
        ];

        $connection = TestUtil::getConnection();

        $container = $this->prophesize(ContainerInterface::class);

        $container->get('my_connection')->willReturn($connection)->shouldBeCalled();
        $container->get('config')->willReturn($config)->shouldBeCalled();
        $container->get(FQCNMessageFactory::class)->willReturn(new FQCNMessageFactory());
        $container->get(PersistenceStrategy\MySqlAggregateStreamStrategy::class)->willReturn($this->prophesize(PersistenceStrategy\MariaDbPersistenceStrategy::class));
        $container->get(MariaDbMetadataLockStrategy::class)->willReturn($this->prophesize(WriteLockStrategy::class))->shouldBeCalled();

        $factory = new MariaDbEventStoreFactory();
        $factory($container->reveal());
    }

    /**
     * @test
     */
    public function it_throws_exception_when_invalid_container_given(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $eventStoreName = 'custom';
        MariaDbEventStoreFactory::$eventStoreName('invalid container');
    }
}
