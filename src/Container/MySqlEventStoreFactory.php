<?php
/**
 * This file is part of the prooph/pdo-event-store.
 * (c) 2016-2017 prooph software GmbH <contact@prooph.de>
 * (c) 2016-2017 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Prooph\EventStore\Pdo\Container;

use Prooph\Common\Event\ProophActionEventEmitter;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\EventStore\ActionEventEmitterEventStore;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\Pdo\MySqlEventStore;

final class MySqlEventStoreFactory extends AbstractEventStoreFactory
{
    protected function createActionEventEmitterEventStore(EventStore $eventStore): ActionEventEmitterEventStore
    {
        return new ActionEventEmitterEventStore(
            $eventStore,
            new ProophActionEventEmitter([
                ActionEventEmitterEventStore::EVENT_APPEND_TO,
                ActionEventEmitterEventStore::EVENT_CREATE,
                ActionEventEmitterEventStore::EVENT_LOAD,
                ActionEventEmitterEventStore::EVENT_LOAD_REVERSE,
                ActionEventEmitterEventStore::EVENT_DELETE,
                ActionEventEmitterEventStore::EVENT_HAS_STREAM,
                ActionEventEmitterEventStore::EVENT_FETCH_STREAM_METADATA,
                ActionEventEmitterEventStore::EVENT_UPDATE_STREAM_METADATA,
                ActionEventEmitterEventStore::EVENT_DELETE_PROJECTION,
                ActionEventEmitterEventStore::EVENT_RESET_PROJECTION,
                ActionEventEmitterEventStore::EVENT_STOP_PROJECTION,
                ActionEventEmitterEventStore::EVENT_FETCH_STREAM_NAMES,
                ActionEventEmitterEventStore::EVENT_FETCH_CATEGORY_NAMES,
                ActionEventEmitterEventStore::EVENT_FETCH_PROJECTION_NAMES,
            ])
        );
    }

    protected function eventStoreClassName(): string
    {
        return MySqlEventStore::class;
    }

    protected function buildConnectionDsn(array $params): string
    {
        $dsn = 'mysql:';

        if (isset($params['host']) && $params['host'] !== '') {
            $dsn .= 'host=' . $params['host'] . ';';
        }

        if (isset($params['port'])) {
            $dsn .= 'port=' . $params['port'] . ';';
        }

        if (isset($params['dbname'])) {
            $dsn .= 'dbname=' . $params['dbname'] . ';';
        }

        if (isset($params['charset'])) {
            $dsn .= 'charset=' . $params['charset'] . ';';
        }

        return $dsn;
    }

    public function defaultOptions(): iterable
    {
        return [
            'connection_options' => [
                'user' => 'root',
                'password' => '',
                'host' => '127.0.0.1',
                'dbname' => 'event_store',
                'port' => 3306,
                'charset' => 'utf8',
            ],
            'load_batch_size' => 1000,
            'event_streams_table' => 'event_streams',
            'projections_table' => 'projections',
            'message_factory' => FQCNMessageFactory::class,
            'wrap_action_event_emitter' => true,
            'metadata_enrichers' => [],
            'plugins' => [],
        ];
    }
}
