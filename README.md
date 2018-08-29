# pdo-event-store

[![Build Status](https://travis-ci.org/prooph/pdo-event-store.svg?branch=master)](https://travis-ci.org/prooph/pdo-event-store)
[![Coverage Status](https://coveralls.io/repos/prooph/pdo-event-store/badge.svg?branch=master&service=github)](https://coveralls.io/github/prooph/pdo-event-store?branch=master)
[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/prooph/improoph)

PDO EventStore implementation for [Prooph EventStore](https://github.com/prooph/event-store)

Requirements
------------

- PHP >= 7.1
- PDO_MySQL Extension or PDO_PGSQL Extension

For MariaDB you need server version >= 10.2.11.  
**Performance Impact**: see [MariaDB Indexes and Efficiency](docs/variants.md#MariaDB Indexes and Efficiency)

For MySQL you need server version >= 5.7.9.

For Postgres you need server version >= 9.4.

Attention: Since v1.6.0 MariaDB Server has to be at least 10.2.11 due to a bugfix in MariaDB, see https://jira.mariadb.org/browse/MDEV-14402.

Setup
-----

For MariaDB run the script in `scripts/mariadb/01_event_streams_table.sql` on your server.

For MySQL run the script in `scripts/mysql/01_event_streams_table.sql` on your server.

For Postgres run the script in `scripts/postgres/01_event_streams_table.sql` on your server.

This will setup the required event streams table.

If you want to use the projections, run additionally the scripts `scripts/mariadb/02_projections_table.sql`
(for MariaDB), `scripts/mysql/02_projections_table.sql` (for MySQL) or
`scripts/postgres/02_projections_table.sql` (for Postgres) on your server.

Upgrade from 1.6 to 1.7
-----------------------

Starting from v1.7 the pdo-event-store uses optimized table schemas.
The upgrade can be done in background with a script optimizing that process.
A downtime for the database should not be needed.
In order to upgrade your existing database, you have to execute:

- MariaDB

```
ALTER TABLE `event_streams` MODIFY `metadata` LONGTEXT NOT NULL;
ALTER TABLE `projections` MODIFY `position` LONGTEXT;
ALTER TABLE `projections` MODIFY `state` LONGTEXT;
```

Then for all event-streams (`SELECT stream_name FROM event_streams`)

```
ALTER TABLE <stream_name> MODIFY `payload` LONGTEXT NOT NULL;
ALTER TABLE <stream_name> MODIFY `metadata` LONGTEXT NOT NULL,
```

- MySQL

nothing to upgrade

- Postgres

For all event-streams (`SELECT stream_name FROM event_streams`)

```
ALTER TABLE <stream_name> MODIFY event_id UUID NOT NULL;
```

Additional note:

When using Postgres, the event_id has to be a valid uuid, so be careful when using a custom MetadataMatcher, as the
event-store could throw an exception when passing a non-valid uuid (f.e. "foo") as uuid.

The migration is strongly recommended, but not required. It's fully backward-compatible. The change on Postgres is
only a microoptimization, the change on MariaDB prevents errors, when the stored json gets too big.

Introduction
------------

[![Prooph Event Store v7](https://img.youtube.com/vi/QhpDIqYQzg0/0.jpg)](https://www.youtube.com/watch?v=QhpDIqYQzg0)

Tests
-----
If you want to run the unit tests locally you need a runnging MySql server listening on port `3306` 
and a running Postgres server listening on port `5432`. Both should contain an empty database `event_store_tests`.

## Run Tests With Composer

### MariaDb

`$ vendor/bin/phpunit -c phpunit.xml.mariadb`

### MySql

`$ vendor/bin/phpunit -c phpunit.xml.mysql`

### Postgres

`$ vendor/bin/phpunit -c phpunit.xml.postgres`

## Run Tests With Docker Compose

### MariaDb

```bash
docker-compose -f docker-compose-tests.yml run composer run-script test-mariadb --timeout 0; \
docker stop prooph-pdo-event-store_mariadb_1 && docker rm prooph-pdo-event-store_mariadb_1
```

### MySql

```bash
docker-compose -f docker-compose-tests.yml run composer run-script test-mysql --timeout 0; \
docker stop prooph-pdo-event-store_mysql_1 && docker rm prooph-pdo-event-store_mysql_1
```

### Postgres

```bash
docker-compose -f docker-compose-tests.yml run composer run-script test-postgres --timeout 0; \
docker stop prooph-pdo-event-store_postgres_1 && docker rm prooph-pdo-event-store_postgres_1
```

## Support

- Ask questions on Stack Overflow tagged with [#prooph](https://stackoverflow.com/questions/tagged/prooph).
- File issues at [https://github.com/prooph/event-store/issues](https://github.com/prooph/event-store/issues).
- Say hello in the [prooph gitter](https://gitter.im/prooph/improoph) chat.

## Contribute

Please feel free to fork and extend existing or add new plugins and send a pull request with your changes!
To establish a consistent code quality, please provide unit tests for all your changes and may adapt the documentation.

## License

Released under the [New BSD License](LICENSE).

