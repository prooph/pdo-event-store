# Prooph PDO Event Store

The PDO Event Store is an implementation of [prooph/event-store](https://github.com/prooph/event-store) that supports
MySQL as well as PostgreSQL.

For a better understanding, we recommend to read the event-store docs, first.

## Differences of MariaDb-/MySql- & PostgresEventStore

The PostgresEventStore has a better performance (at least with default database configuration) and implements the
`TransactionalEventStore` interface. If you need maximum performance or transaction support, we recommend to use the
PostgresEventStore over the MariaDb-/MySqlEventStore.

## Event streams / projections table

All known event streams are stored in an event stream table, so with a simple table lookup, you can find out what streams
are available in your store.

Same goes for the projections, all known projections are stored in a single table, so you can see what projections are
available, and what their current state / stream positition / status is.

## Load batch size

When reading from an event streams with multiple aggregates (especially when using projections), you could end of with
millions of events loaded in memory. Therefor the pdo-event-store will load events only in batches of 1000 by default.
You can change to value to something higher to achieve even more performance with higher memory usage, or decrease it
to reduce memory usage even more, with the drawback of having a not as good performance.

## PDO Connection for event-store and projection manager

It is important to use the same database for event-store and projection manager, you could use distinct pdo connections
if you want to, but they should be both connected to the same database. Otherwise you will run into issues, because the
projection manager needs to query the underlying database table of the event-store for its querying API.
 
It's recommended to just use the same pdo connection instance for both.

## Persistance Strategies

This component ships with 9 default persistance strategies:

- MariaDbAggregateStreamStrategy
- MariaDbSimpleStreamStrategy
- MariaDbSingleStreamStrategy
- MySqlAggregateStreamStrategy
- MySqlSimpleStreamStrategy
- MySqlSingleStreamStrategy
- PostgresAggregateStreamStrategy
- PostgresSimpleStreamStrategy
- PostgresSingleStreamStrategy

All persistance strategies have the following in common:

The generated table name for a given stream is:

`'_' . sha1($streamName->toString()`

so a sha1 hash of the stream name, prefixed with an underscore is the used table name.
You can query the `event_streams` table to get real stream name to stream name mapping.

You can implement your own persistance strategy by implementing the `Prooph\EventStore\Pdo\PersistenceStrategy` interface.

### AggregateStreamStrategy

This stream strategy should be used together with event-sourcing, if you use one stream per aggregate. For example, you have 2 instances of two
different aggregates named `user-123`, `user-234`, `todo-345` and `todo-456`, you would have 4 different event streams,
one for each aggregate.

This stream strategy is the most performant of all, but it will create a lot of database tables, which is something not
everyone likes (especially DB admins).

All needed database tables will be created automatically for you.

### SingleStreamStrategy

This stream strategy should be used together with event-sourcing, if you want to store all events of an aggregate type into a single stream, for example
`user-123` and `user-234` should both be stored into a stream called `user`.

You can also store all stream of all aggregate types into a single stream, for example your aggregates `user-123`,
`user-234`, `todo-345` and `todo-456` can all be stored into a stream called `event_stream`.

This stream strategy is slightly less performant then the aggregate stream strategy.

You need to setup the database table yourself when using this strategy. An example script to do that can be [found here](https://github.com/prooph/proophessor-do/blob/master/scripts/create_event_stream.php).

### SimpleStreamStrategy

This stream strategy is not meant to be used for event-sourcing. It will create simple event streams without any constraints
at all, so having two events of the same aggregate with the same version will not rise any error.

This is very useful for projections, where you copy events from one stream to another (the resulting stream may need to use
the simple stream strategy) or when you want to use the event-store outside the scope of event-sourcing.

You need to setup the database table yourself when using this strategy. An example script to do that can be [found here](https://github.com/prooph/proophessor-do/blob/master/scripts/create_event_stream.php).

### Using custom stream strategies

When you query the event streams a lot, it might be a good idea to create your own stream strategy, so you can add
custom indexes to your database tables. When using with the MetadataMatcher, take care that you add the metadata
matches in the right order, so they can match your indexes.

### Disable transaction handling

You can configure the event store to disable transaction handling completely. In order to do this, set the last parameter
in the constructor to true (or configure your interop config factory accordingly, key is `disable_transaction_handling`).

Enabling this feature will disable all transaction handling and you have to take care yourself to start, commit and rollback
transactions.

Note: This could lead to problems using the event store, if you did not manage to handle the transaction handling accordingly.
This is your problem and we will not provide any support for problems you encounter while doing so.
