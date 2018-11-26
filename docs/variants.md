# Variants

The PDO Event Store is an implementation of [prooph/event-store](https://github.com/prooph/event-store) that supports
MySQL and MariaDB as well as PostgreSQL.

For a better understanding, we recommend to read the event-store docs, first.

## Differences of MariaDb-/MySql- & PostgresEventStore

The PostgresEventStore has a better performance (at least with default database configuration) and implements the
`TransactionalEventStore` interface. If you need maximum performance or transaction support, we recommend to use the
PostgresEventStore instead of the MariaDb-/MySqlEventStore.

## Event streams / projections table

All known event streams are stored in an event stream table, so with a simple table lookup, you can find out what streams
are available in your store.

Same goes for the projections, all known projections are stored in a single table, so you can see what projections are
available, and what their current state / stream position / status is.

## Load batch size

When reading from an event streams with multiple aggregates (especially when using projections), you could end of with
millions of events loaded in memory. Therefor the pdo-event-store will load events only in batches of 10000 by default.
You can change to value to something higher to achieve even more performance with higher memory usage, or decrease it
to reduce memory usage even more, with the drawback of having a not as good performance.

## PDO Connection for event-store and projection manager

It is important to use the same database for event-store and projection manager, you could use distinct pdo connections
if you want to, but they should be both connected to the same database. Otherwise you will run into issues, because the
projection manager needs to query the underlying database table of the event-store for its querying API.
 
It's recommended to just use the same pdo connection instance for both.

## Persistence Strategies

This component ships with 9 default persistence strategies:

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

You can implement your own persistence strategy by implementing the `Prooph\EventStore\Pdo\PersistenceStrategy` interface.

### AggregateStreamStrategy

This stream strategy should be used together with event-sourcing, if you use one stream per aggregate. For example, you have 2 instances of two
different aggregates named `user-123`, `user-234`, `todo-345` and `todo-456`, you would have 4 different event streams,
one for each aggregate.

This stream strategy is the most performant of all (with downsides, see notes), but it will create a lot of database tables, which is something not
everyone likes (especially DB admins).

All needed database tables will be created automatically for you.

Note: For event-store projections the aggregate stream strategy is not that performant anymore, consider using [CategoryStreamProjectionRunner](https://github.com/prooph/standard-projections/blob/master/src/CategoryStreamProjectionRunner.php) from the [standard-projections](https://github.com/prooph/standard-projections) repository.
But even than, the projections would be slow, because the projector needs to check all the streams one-by-one for any new events. Because of this speed of finding and projecting any new events depends on the number of streams which means it would rapidly decrease as you add more data to your event store.

You could however drastically improve the projections, if you would add a category stream projection as event-store-plugin. (This doesn't exist, yet)

### SingleStreamStrategy

This stream strategy should be used together with event-sourcing, if you want to store all events of an aggregate type into a single stream, for example
`user-123` and `user-234` should both be stored into a stream called `user`.

You can also store all stream of all aggregate types into a single stream, for example your aggregates `user-123`,
`user-234`, `todo-345` and `todo-456` can all be stored into a stream called `event_stream`.

This stream strategy is slightly less performant then the aggregate stream strategy.

You need to setup the database table yourself when using this strategy. An example script to do that can be [found here](https://github.com/prooph/proophessor-do/blob/master/scripts/create_event_streams.php).

### SimpleStreamStrategy

This stream strategy is not meant to be used for event-sourcing. It will create simple event streams without any constraints
at all, so having two events of the same aggregate with the same version will not rise any error.

This is very useful for projections, where you copy events from one stream to another (the resulting stream may need to use
the simple stream strategy) or when you want to use the event-store outside the scope of event-sourcing.

You need to setup the database table yourself when using this strategy. An example script to do that can be [found here](https://github.com/prooph/proophessor-do/blob/master/scripts/create_event_streams.php).

### Using custom stream strategies

When you query the event streams a lot, it might be a good idea to create your own stream strategy, so you can add
custom indexes to your database tables. When using with the MetadataMatcher, take care that you add the metadata
matches in the right order, so they can match your indexes.

### MariaDB Indexes and Efficiency

Unlike MySQL, MariaDB does not use indexed generated columns on the json document, leading to queries not using the 
pre-created indexes and causing a performance drawback.
To fix that, make sure that your `CustomMariaDBPersistencyStrategy` implements the newly introduced 
[`MariaDBIndexedPersistenceStrategy`](https://github.com/prooph/pdo-event-store/blob/master/src/MariaDBIndexedPersistenceStrategy.php) 

### Disable transaction handling

You can configure the event store to disable transaction handling completely. In order to do this, set the last parameter
in the constructor to true (or configure your interop config factory accordingly, key is `disable_transaction_handling`).

Enabling this feature will disable all transaction handling and you have to take care yourself to start, commit and rollback
transactions.

Note: This could lead to problems using the event store, if you did not manage to handle the transaction handling accordingly.
This is your problem and we will not provide any support for problems you encounter while doing so.

### A note on Json

MySql differs from the other vendors in a subtile manner which basicly is a result of the json specification itself. Json 
does not distuinguish between *integers* and *floats*, it just knowns a *number*. This means that when you send a float 
such as `2.0` to the store it will be stored by MySQL as integer `2`. While we have looked at ways to prevent this we 
decided it would become too complicated to support that (could be done with nested JSON_OBJECT calls, which strangely 
does store such value as-is).

We think you can easily avoid this from becoming an issue by ensuring your events handle such differences. 

Example

```php
final class MySliderChanged extends AggregateChanged
{
    public static function with(MySlider $slider): self {
        return self::occur((string) $dossierId, [
            'value' => $slider->toNative(), // float
        ]);
    }
    
    public function mySlider(): MySlider
    {
        return MySlider::from((float) $this->payload['value']); // use casting
    }
}
```
