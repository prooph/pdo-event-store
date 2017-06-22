# pdo-event-store

[![Build Status](https://travis-ci.org/prooph/pdo-event-store.svg?branch=master)](https://travis-ci.org/prooph/pdo-event-store)
[![Coverage Status](https://coveralls.io/repos/prooph/pdo-event-store/badge.svg?branch=master&service=github)](https://coveralls.io/github/prooph/pdo-event-store?branch=master)
[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/prooph/improoph)

PDO EventStore implementation for [Prooph EventStore](https://github.com/prooph/event-store)

Requirements
------------

- PHP >= 7.1
- PDO_MySQL Extension or PDO_PGSQL Extension

For MariaDB you need server vesion >= 10.2.6.
For MySQL you need server version >= 5.7.9.
For Postgres you need server version >= 9.4.

Setup
-----

For MariaDB run the script in `scripts/mariadb/01_event_streams_table.sql` on your server.

For MySQL run the script in `scripts/mysql/01_event_streams_table.sql` on your server.

For Postgres run the script in `scripts/postgres/01_event_streams_table.sql` on your server.

This will setup the required event streams table.

If you want to use the projections, run additionally the scripts `scripts/mariadb/02_projections_table.sql`
(for MariaDB), `scripts/mysql/02_projections_table.sql` (for MySQL) or
`scripts/postgres/02_projections_table.sql` (for Postgres) on your server.

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

`$ docker-compose -f docker-compose-tests.yml run --rm composer run-script test-mariadb --timeout 0`

### MySql

`$ docker-compose -f docker-compose-tests.yml run --rm composer run-script test-mysql --timeout 0`

### Postgres

`$ docker-compose -f docker-compose-tests.yml run --rm composer run-script test-postgres --timeout 0`
