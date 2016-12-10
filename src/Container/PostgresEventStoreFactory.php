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
use Prooph\EventStore\PDO\PostgresEventStore;
use Prooph\EventStore\TransactionalActionEventEmitterEventStore;

final class PostgresEventStoreFactory extends AbstractEventStoreFactory
{
    protected function createActionEventEmitter(): ActionEventEmitter
    {
        return new ProophActionEventEmitter([
            TransactionalActionEventEmitterEventStore::EVENT_APPEND_TO,
            TransactionalActionEventEmitterEventStore::EVENT_CREATE,
            TransactionalActionEventEmitterEventStore::EVENT_LOAD,
            TransactionalActionEventEmitterEventStore::EVENT_LOAD_REVERSE,
            TransactionalActionEventEmitterEventStore::EVENT_DELETE,
            TransactionalActionEventEmitterEventStore::EVENT_HAS_STREAM,
            TransactionalActionEventEmitterEventStore::EVENT_FETCH_STREAM_METADATA,
            TransactionalActionEventEmitterEventStore::EVENT_BEGIN_TRANSACTION,
            TransactionalActionEventEmitterEventStore::EVENT_COMMIT,
            TransactionalActionEventEmitterEventStore::EVENT_ROLLBACK,
        ]);
    }

    protected function eventStoreClassName(): string
    {
        return PostgresEventStore::class;
    }

    public function defaultOptions(): iterable
    {
        return [
            'connection_options' => [
                'driver' => 'pdo_pgsql',
                'user' => 'postgres',
                'password' => 'postgres',
                'host' => '127.0.0.1',
                'dbname' => 'event_store',
                'port' => 5432,
            ],
            'load_batch_size' => 1000,
            'event_streams_table' => 'event_streams',
            'message_converter' => NoOpMessageConverter::class,
            'message_factory' => FQCNMessageFactory::class,
        ];
    }
}
