<?php
/**
 * This file is part of the prooph/event-store-mysql-adapter.
 * (c) 2016-2016 prooph software GmbH <contact@prooph.de>
 * (c) 2016-2016 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace ProophTest\EventStore\Adapter\MySQL\Service;

use Interop\Container\ContainerInterface;
use PHPUnit_Framework_TestCase as TestCase;
use Prooph\EventStore\Adapter\MySQL\MySQLEventStoreAdapter;
use Prooph\EventStore\Adapter\MySQL\Container\MySQLEventStoreAdapterFactory;

final class MySQLEventStoreAdapterFactoryTest extends TestCase
{
    /**
     * @test
     */
    public function it_creates_adapter(): void
    {
        $manager = new Manager('MySQL://localhost:27017');
        $dbName = 'mongo_adapter_test';

        $config = [];
        $config['prooph']['event_store']['adapter']['options'] = [
            'mongo_manager' => 'mongo_manager',
            'db_name' => $dbName,
        ];

        $mock = $this->getMockForAbstractClass(ContainerInterface::class);
        $mock->expects($this->at(0))->method('get')->with('config')->will($this->returnValue($config));
        $mock->expects($this->at(1))->method('get')->with('mongo_manager')->will($this->returnValue($manager));

        $factory = new MySQLEventStoreAdapterFactory();
        $adapter = $factory($mock);

        $this->assertInstanceOf(MySQLEventStoreAdapter::class, $adapter);
    }
}
