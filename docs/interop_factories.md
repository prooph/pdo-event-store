# Interop Factories

Instead of providing a module, a bundle, a bridge or similar framework integration prooph/event-store ships with `interop factories`.

## Factory-Driven Creation

The concept behind these factories (see `src/Container` folder) is simple but powerful. It allows us to provide you with bootstrapping logic for the event store and related components
without the need to rely on a specific framework. However, the factories have three requirements.

### Requirements

1. Your Inversion of Control container must implement the [PSR Container interface](https://github.com/php-fig/container).
2. [interop-config](https://github.com/sandrokeil/interop-config) must be installed
3. The application configuration should be registered with the service id `config` in the container.

*Note: Don't worry, if your environment doesn't provide these requirements, you can
always bootstrap the components by hand. Just look at the factories for inspiration in this case.*

### MariaDbEventStoreFactory

If the requirements are met you just need to add a new section in your application config ...

```php
[
    'prooph' => [
        'event_store' => [
            'default' => [
                'wrap_action_event_emitter' => true,
                'metadata_enrichers' => [
                    // The factory will get the metadata enrichers and inject them in the MetadataEnricherPlugin.
                    // Note: you can obtain the same result by instanciating the plugin yourself
                    // and pass it to the 'plugin' section bellow.
                    'metadata_enricher_1',
                    'metadata_enricher_2',
                    // ...
                ],
                'plugins' => [
                    //And again the factory will use each service id to get the plugin from the container
                    //Plugin::attachToEventStore($eventStore) is then invoked by the factory so your plugins
                    // get attached automatically
                    //Awesome, isn't it?
                    'plugin_1_service_id',
                    'plugin_2_service_id',
                    //...
                ],
                'connection' => 'my_pdo_connection', // service id for the used pdo connection
                'persistence_strategy' => MariaDbSingleStreamStrategy::class, // service id for the used persistance strategy
                'load_batch_size' => 1000, // how many events a query should return in one batch, defaults to 1000
                'event_streams_table' => 'event_streams', // event stream table to use, defaults to `event_streams`
                'message_factory' => FQCNMessageFactory::class, // message factory to use, defauls to `FQCNMessageFactory::class`
            ],
        ],
    ],
    'dependencies' => [
        'factories' => [
            'MariaDbEventStore' => [
                \Prooph\EventStore\Container\MariaDbEventStoreFactory::class,
                'default',
            ],
        ],
    ],
    //... other application config here
]
```

$eventStore = $container->get('MariaDbEventStore');

### MariaDbProjectionManagerFactory

```php
[
    'prooph' => [
        'projection_manager' => [
            'default' => [
                'event_store' => 'MariaDbEventStore',
                'connection' => 'my_pdo_connection', // service id for the used pdo connection
                'event_streams_table' => 'event_streams', // event stream table to use, defaults to `event_streams` 
                'projections_table' => 'projections', // projection table to use, defaults to `projections`
            ],
        ],
    ],
    'dependencies' => [
        'factories' => [
            'MariaDbProjectionManager' => [
                \Prooph\EventStore\Container\MariaDbProjectionManagerFactory::class,
                'default',
            ],
        ],
    ],
    //... other application config here
]
```

$projectionManager = $container->get('MariaDbProjectionManager');

### MySqlEventStoreFactory

If the requirements are met you just need to add a new section in your application config ...

```php
[
    'prooph' => [
        'event_store' => [
            'default' => [
                'wrap_action_event_emitter' => true,
                'metadata_enrichers' => [
                    // The factory will get the metadata enrichers and inject them in the MetadataEnricherPlugin.
                    // Note: you can obtain the same result by instanciating the plugin yourself
                    // and pass it to the 'plugin' section bellow.
                    'metadata_enricher_1',
                    'metadata_enricher_2',
                    // ...
                ],
                'plugins' => [
                    //And again the factory will use each service id to get the plugin from the container
                    //Plugin::attachToEventStore($eventStore) is then invoked by the factory so your plugins
                    // get attached automatically
                    //Awesome, isn't it?
                    'plugin_1_service_id',
                    'plugin_2_service_id',
                    //...
                ],
                'connection' => 'my_pdo_connection', // service id for the used pdo connection
                'persistence_strategy' => MySqlSingleStreamStrategy::class, // service id for the used persistance strategy
                'load_batch_size' => 1000, // how many events a query should return in one batch, defaults to 1000
                'event_streams_table' => 'event_streams', // event stream table to use, defaults to `event_streams`
                'message_factory' => FQCNMessageFactory::class, // message factory to use, defauls to `FQCNMessageFactory::class`
            ],
        ],
    ],
    'dependencies' => [
        'factories' => [
            'MySqlEventStore' => [
                \Prooph\EventStore\Container\MySqlEventStoreFactory::class,
                'default',
            ],
        ],
    ],
    //... other application config here
]
```

$eventStore = $container->get('MySqlEventStore');

### MySqlProjectionManagerFactory

```php
[
    'prooph' => [
        'projection_manager' => [
            'default' => [
                'event_store' => 'MySqlEventStore',
                'connection' => 'my_pdo_connection', // service id for the used pdo connection
                'event_streams_table' => 'event_streams', // event stream table to use, defaults to `event_streams` 
                'projections_table' => 'projections', // projection table to use, defaults to `projections`
            ],
        ],
    ],
    'dependencies' => [
        'factories' => [
            'MySqlProjectionManager' => [
                \Prooph\EventStore\Container\MySqlProjectionManagerFactory::class,
                'default',
            ],
        ],
    ],
    //... other application config here
]
```

$projectionManager = $container->get('MySqlProjectionManager');

### PostgresEventStoreFactory

If the requirements are met you just need to add a new section in your application config ...

```php
[
    'prooph' => [
        'event_store' => [
            'default' => [
                'wrap_action_event_emitter' => true,
                'metadata_enrichers' => [
                    // The factory will get the metadata enrichers and inject them in the MetadataEnricherPlugin.
                    // Note: you can obtain the same result by instanciating the plugin yourself
                    // and pass it to the 'plugin' section bellow.
                    'metadata_enricher_1',
                    'metadata_enricher_2',
                    // ...
                ],
                'plugins' => [
                    //And again the factory will use each service id to get the plugin from the container
                    //Plugin::attachToEventStore($eventStore) is then invoked by the factory so your plugins
                    // get attached automatically
                    //Awesome, isn't it?
                    'plugin_1_service_id',
                    'plugin_2_service_id',
                    //...
                ],
                'connection' => 'my_pdo_connection', // service id for the used pdo connection
                'persistence_strategy' => PostgresSingleStreamStrategy::class, // service id for the used persistance strategy
                'load_batch_size' => 1000, // how many events a query should return in one batch, defaults to 1000
                'event_streams_table' => 'event_streams', // event stream table to use, defaults to `event_streams`
                'message_factory' => FQCNMessageFactory::class, // message factory to use, defauls to `FQCNMessageFactory::class`
            ],
        ],
    ],
    'dependencies' => [
        'factories' => [
            'PostgresEventStore' => [
                \Prooph\EventStore\Container\PostgresEventStoreFactory::class,
                'default',
            ],
        ],
    ],
    //... other application config here
]
```

$eventStore = $container->get('PostgresEventStore');

### PostgresProjectionManagerFactory

```php
[
    'prooph' => [
        'projection_manager' => [
            'default' => [
                'event_store' => 'PostgresEventStore',
                'connection' => 'my_pdo_connection', // service id for the used pdo connection
                'event_streams_table' => 'event_streams', // event stream table to use, defaults to `event_streams` 
                'projections_table' => 'projections', // projection table to use, defaults to `projections`
            ],
        ],
    ],
    'dependencies' => [
        'factories' => [
            'PostgresProjectionManager' => [
                \Prooph\EventStore\Container\PostgresProjectionManagerFactory::class,
                'default',
            ],
        ],
    ],
    //... other application config here
]
```

$projectionManager = $container->get('PostgresProjectionManager');

### PDO-based event stores

The three PDO-based event stores (`MySqlEventStoreFactory`, `MariaDbEventStoreFactory`and `PostgresEventStoreFactory`)
share the same config options:

* `connection` (**required**): The ID of your PDO service
* `persistence_strategy` (**required**): The ID of your persistence strategy service. You can learn more about
persistence strategies [here](/event-store/implementations/pdo_event_store/variants.html#persistence-strategies).
* `load_batch_size` (*default: 1000*): This is the maximum number of events retrieved when calling the `load` method.
* `event_streams_table` (*default: `event_streams`*): The name of the table where event streams metadata are persisted. Note that this is not the
table where the stream of events is persisted.
* `message_factory` (*default: [FQCNMessageFactory](https://github.com/prooph/common/blob/master/src/Messaging/FQCNMessageFactory.php)*):
The ID of a service implementing [`MessageFactory` interface](https://github.com/prooph/common/blob/master/src/Messaging/MessageFactory.php#L15).
* `wrap_action_event_emitter` (*default: true*): Decorate the event store with an `ActionEventEmitterEventStore`.
This is needed if you want to use plugins (see more details [here](http://docs.getprooph.org/event-store/event_store_plugins.html)).
* `metadata_enrichers` (*default: []*): A list of IDs of services implementing the [`MetadataEnricher` interface](https://github.com/prooph/event-store/blob/master/src/Metadata/MetadataEnricher.php).
`wrap_action_event_emitter` has to be enabled. For more details about metadata enrichers, see [here](event_store_plugins.html#metadata-enrichier).
* `plugins` (*default: []*): A list of IDs of services implementing the [`Plugin` interface](https://github.com/prooph/event-store/blob/master/src/Plugin/Plugin.php).
`wrap_action_event_emitter` has to be enabled. For more details about plugins, see [here](event_store_plugins.html).
* `disable_transaction_handling` (*default: false*): Disable SQL transactions.

#### PDOConnectionFactory

In addition to event store factories, a `PDOConnectionFactory` is also provided. It supports following config options,
under `prooph.pdo_connection` dimension:

* `schema` (**required**): Type of the database (either `mysql` or `pgsql`).
* `host` (*default: 127.0.0.1*)
* `port` (**required**)
* `user` (**required**)
* `password` (**required**)
* `dbname` (*default: event_store*)
* `charset` (*default: utf8*)

```php
[
    'prooph' => [
        'pdo_connection' => [
            'default' => [
                'schema', // one of mysql or pgsql
                'user', // username to use
                'password', // password to use
                'port', // port to use
                'host' => '127.0.0.1', // host name, defaults to `127.0.0.1`
                'dbname' => 'event_store', // database name, defaults to `event_store`
                'charset' => 'utf8', // chartset, defaults to `UTF-8`,
            ],
        ],
    ],
    'dependencies' => [
        'factories' => [
            'my_pdo_connection' => [
                \Prooph\EventStore\Container\PdoConnectionFactory::class,
                'default',
            ],
        ],
    ],
    //... other application config here
]
```

$pdoConnection = $container->get('my_pdo_connection');
