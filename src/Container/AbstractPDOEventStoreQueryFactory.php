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

namespace Prooph\EventStore\PDO\Container;

use Interop\Container\ContainerInterface;
use Prooph\EventStore\PDO\Projection\AbstractPDOQuery;

abstract class AbstractPDOEventStoreQueryFactory extends AbstractProjectionFactory
{
    abstract protected function getInstanceClassName(): string;

    public function __invoke(ContainerInterface $container): AbstractPDOQuery
    {
        $config = $container->get('config');
        $config = $this->options($config, $this->configId);

        $connection = $this->getConnection($container, $config);
        $eventStore = $this->getEventStore($container, $config);

        $instanceClassName = $this->getInstanceClassName();
        return new $instanceClassName(
            $eventStore,
            $connection,
            $config['event_streams_table']
        );
    }

    public function defaultOptions(): array
    {
        return [
            'connection_options' => [
                'driver' => 'pdo_mysql',
                'user' => 'root',
                'password' => '',
                'host' => '127.0.0.1',
                'dbname' => 'event_store',
                'port' => 3306,
            ],
            'event_streams_table' => 'event_streams',
        ];
    }

    public function mandatoryOptions(): array
    {
        return [
            'event_store',
        ];
    }
}
