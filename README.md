# pdo-event-store

[![Build Status](https://travis-ci.org/prooph/pdo-event-store.svg?branch=master)](https://travis-ci.org/prooph/pdo-event-store)
[![Coverage Status](https://coveralls.io/repos/prooph/pdo-event-store/badge.svg?branch=master&service=github)](https://coveralls.io/github/prooph/pdo-event-store?branch=master)
[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/prooph/improoph)

PDO EventStore implementation for [Prooph EventStore](https://github.com/prooph/event-store)

Requirements
------------

- PHP >= 7.1
- PDO_MySQL Extension or PDO_PGSQL Extension

For MySQL you need server version >= 5.7.
For Postgres you need server version >= 9.4.

Setup
-----

For MySQL run the script in `scripts/mysql_event_streams_table.sql` on your server.

For Postgres run the script in `scripts/postgres_event_streams_table` on your server.

This will setup the required event streams table.

If you want to use the projections, run additionally the scripts `scripts/mysql_projections_table.sql` (for MySQL)
or `scripts/postgres_projections_table.sql` (for Postgres) on your server.
