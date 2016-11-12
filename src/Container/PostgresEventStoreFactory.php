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
use Prooph\EventStore\CanControlTransactionActionEventEmitterAware;
use Prooph\EventStore\PDO\PostgresEventStore;
use Prooph\EventStore\PDO\TableNameGeneratorStrategy\Sha1;

final class PostgresEventStoreFactory extends AbstractEventStoreFactory
{
    protected function createActionEventEmitter(): ActionEventEmitter
    {
        return new ProophActionEventEmitter([
            CanControlTransactionActionEventEmitterAware::EVENT_APPEND_TO,
            CanControlTransactionActionEventEmitterAware::EVENT_CREATE,
            CanControlTransactionActionEventEmitterAware::EVENT_LOAD,
            CanControlTransactionActionEventEmitterAware::EVENT_LOAD_REVERSE,
            CanControlTransactionActionEventEmitterAware::EVENT_BEGIN_TRANSACTION,
            CanControlTransactionActionEventEmitterAware::EVENT_COMMIT,
            CanControlTransactionActionEventEmitterAware::EVENT_ROLLBACK,
        ]);
    }

    protected function eventStoreClassName(): string
    {
        return PostgresEventStore::class;
    }


    public function defaultOptions(): array
    {
        return [
            'adapter' => [
                'options' => [
                    'connection_options' => [
                        'driver' => 'pdo_pgsql',
                        'user' => 'postgres',
                        'password' => 'postgres',
                        'host' => '127.0.0.1',
                        'dbname' => 'event_store',
                        'port' => 5432,
                    ],
                    'table_name_generator_strategy' => Sha1::class,
                    'load_batch_size' => 1000,
                    'event_streams_table' => 'event_streams',
                    'message_converter' => NoOpMessageConverter::class,
                    'message_factory' => FQCNMessageFactory::class,
                ],
            ],
        ];
    }
}
