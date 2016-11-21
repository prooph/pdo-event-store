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

use Prooph\Common\Event\ActionEventEmitter;
use Prooph\Common\Event\ProophActionEventEmitter;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\ActionEventEmitterAware;
use Prooph\EventStore\PDO\MySQLEventStore;
use Prooph\EventStore\PDO\TableNameGeneratorStrategy\Sha1;

final class MySQLEventStoreFactory extends AbstractEventStoreFactory
{
    protected function createActionEventEmitter(): ActionEventEmitter
    {
        return new ProophActionEventEmitter([
            ActionEventEmitterAware::EVENT_APPEND_TO,
            ActionEventEmitterAware::EVENT_CREATE,
            ActionEventEmitterAware::EVENT_LOAD,
            ActionEventEmitterAware::EVENT_LOAD_REVERSE,
            ActionEventEmitterAware::EVENT_DELETE,
            ActionEventEmitterAware::EVENT_HAS_STREAM,
            ActionEventEmitterAware::EVENT_FETCH_STREAM_METADATA,
        ]);
    }

    protected function eventStoreClassName(): string
    {
        return MySQLEventStore::class;
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
            'table_name_generator_strategy' => Sha1::class,
            'load_batch_size' => 1000,
            'event_streams_table' => 'event_streams',
            'message_converter' => NoOpMessageConverter::class,
            'message_factory' => FQCNMessageFactory::class,
        ];
    }
}
