# Change Log

## [v1.5.0](https://github.com/prooph/pdo-event-store/tree/v1.5.0)

[Full Changelog](https://github.com/prooph/pdo-event-store/compare/v1.4.1...v1.5.0)

**Implemented enhancements:**

- Flag / Method to turn off transaction handling [\#102](https://github.com/prooph/pdo-event-store/issues/102)
- Disable transaction handling [\#101](https://github.com/prooph/pdo-event-store/pull/101) ([prolic](https://github.com/prolic))

## [v1.4.1](https://github.com/prooph/pdo-event-store/tree/v1.4.1) (2017-07-10)
[Full Changelog](https://github.com/prooph/pdo-event-store/compare/v1.4.0...v1.4.1)

**Implemented enhancements:**

- Replace real\_stream\_name LIKE ? with category = ? IN projectors and queries [\#97](https://github.com/prooph/pdo-event-store/issues/97)
- optimize fromCategories event store queries [\#100](https://github.com/prooph/pdo-event-store/pull/100) ([prolic](https://github.com/prolic))

**Fixed bugs:**

- \[Projections\] ERROR:  value too long for type character\(26\) [\#98](https://github.com/prooph/pdo-event-store/issues/98)
- fix lock until string [\#99](https://github.com/prooph/pdo-event-store/pull/99) ([prolic](https://github.com/prolic))

## [v1.4.0](https://github.com/prooph/pdo-event-store/tree/v1.4.0) (2017-07-03)
[Full Changelog](https://github.com/prooph/pdo-event-store/compare/v1.3.3...v1.4.0)

**Implemented enhancements:**

- Optionally trigger pcntl\_signal\_dispatch [\#96](https://github.com/prooph/pdo-event-store/pull/96) ([fritz-gerneth](https://github.com/fritz-gerneth))

## [v1.3.3](https://github.com/prooph/pdo-event-store/tree/v1.3.3) (2017-06-29)
[Full Changelog](https://github.com/prooph/pdo-event-store/compare/v1.3.2...v1.3.3)

**Fixed bugs:**

- update lock after sleep [\#95](https://github.com/prooph/pdo-event-store/pull/95) ([prolic](https://github.com/prolic))

## [v1.3.2](https://github.com/prooph/pdo-event-store/tree/v1.3.2) (2017-06-24)
[Full Changelog](https://github.com/prooph/pdo-event-store/compare/v1.3.1...v1.3.2)

**Implemented enhancements:**

- make use of projection not found exception [\#94](https://github.com/prooph/pdo-event-store/pull/94) ([prolic](https://github.com/prolic))

## [v1.3.1](https://github.com/prooph/pdo-event-store/tree/v1.3.1) (2017-06-22)
[Full Changelog](https://github.com/prooph/pdo-event-store/compare/v1.3.0...v1.3.1)

**Implemented enhancements:**

- Remove JSON\_FORCE\_OBJECT [\#93](https://github.com/prooph/pdo-event-store/pull/93) ([prolic](https://github.com/prolic))

**Closed issues:**

- Invalid DB\_HOST in phpunit files when using docker-compose to run the tests. [\#90](https://github.com/prooph/pdo-event-store/issues/90)
- Unreasonable use of JSON\_FORCE\_OBJECT in PersistenceStrategies' implementations [\#87](https://github.com/prooph/pdo-event-store/issues/87)

**Merged pull requests:**

- Documentation update for running tests. [\#92](https://github.com/prooph/pdo-event-store/pull/92) ([bweston92](https://github.com/bweston92))
- Fix path to phpunit files from composer.json [\#89](https://github.com/prooph/pdo-event-store/pull/89) ([bweston92](https://github.com/bweston92))
- Remove port bindings from docker-compose [\#88](https://github.com/prooph/pdo-event-store/pull/88) ([bweston92](https://github.com/bweston92))

## [v1.3.0](https://github.com/prooph/pdo-event-store/tree/v1.3.0) (2017-06-18)
[Full Changelog](https://github.com/prooph/pdo-event-store/compare/v1.2.0...v1.3.0)

**Implemented enhancements:**

- Reconsider adding support for MariaDB [\#72](https://github.com/prooph/pdo-event-store/issues/72)
- Add MariaDB support [\#83](https://github.com/prooph/pdo-event-store/pull/83) ([prolic](https://github.com/prolic))
- add event position if field not occupied [\#82](https://github.com/prooph/pdo-event-store/pull/82) ([prolic](https://github.com/prolic))

**Fixed bugs:**

- fix for load/save within same transaction [\#85](https://github.com/prooph/pdo-event-store/pull/85) ([prolic](https://github.com/prolic))

**Closed issues:**

- Postgres error on fetching an aggregate on a fresh eventstore [\#84](https://github.com/prooph/pdo-event-store/issues/84)

**Merged pull requests:**

- Test against PostgreSQL 9.6 [\#81](https://github.com/prooph/pdo-event-store/pull/81) ([dragosprotung](https://github.com/dragosprotung))

## [v1.2.0](https://github.com/prooph/pdo-event-store/tree/v1.2.0) (2017-05-29)
[Full Changelog](https://github.com/prooph/pdo-event-store/compare/v1.1.0...v1.2.0)

## [v1.1.0](https://github.com/prooph/pdo-event-store/tree/v1.1.0) (2017-05-24)
[Full Changelog](https://github.com/prooph/pdo-event-store/compare/v1.0.1...v1.1.0)

**Implemented enhancements:**

- Support message property filters + new operators [\#80](https://github.com/prooph/pdo-event-store/pull/80) ([prolic](https://github.com/prolic))
- Catch PDOExceptions and check error codes [\#78](https://github.com/prooph/pdo-event-store/pull/78) ([dragosprotung](https://github.com/dragosprotung))

**Closed issues:**

- Improve documentation SingleStreamStrategy [\#73](https://github.com/prooph/pdo-event-store/issues/73)

**Merged pull requests:**

- improve docs [\#79](https://github.com/prooph/pdo-event-store/pull/79) ([prolic](https://github.com/prolic))

## [v1.0.1](https://github.com/prooph/pdo-event-store/tree/v1.0.1) (2017-05-17)
[Full Changelog](https://github.com/prooph/pdo-event-store/compare/v1.0.0...v1.0.1)

**Fixed bugs:**

- Catch and ignore duplicate projection error when creating it [\#77](https://github.com/prooph/pdo-event-store/pull/77) ([dragosprotung](https://github.com/dragosprotung))

## [v1.0.0](https://github.com/prooph/pdo-event-store/tree/v1.0.0) (2017-03-30)
[Full Changelog](https://github.com/prooph/pdo-event-store/compare/v1.0.0-beta4...v1.0.0)

**Implemented enhancements:**

- remove uniqueViolationErrorCodes from persistance strategy + add docs [\#71](https://github.com/prooph/pdo-event-store/pull/71) ([prolic](https://github.com/prolic))
- remove lock timeout option \(added to event-store interface\) [\#69](https://github.com/prooph/pdo-event-store/pull/69) ([prolic](https://github.com/prolic))

**Closed issues:**

- Provide new beta release [\#63](https://github.com/prooph/pdo-event-store/issues/63)
- Add documentation [\#31](https://github.com/prooph/pdo-event-store/issues/31)

## [v1.0.0-beta4](https://github.com/prooph/pdo-event-store/tree/v1.0.0-beta4) (2017-03-13)
[Full Changelog](https://github.com/prooph/pdo-event-store/compare/v1.0.0-beta3...v1.0.0-beta4)

**Implemented enhancements:**

- Add projection manager factories [\#60](https://github.com/prooph/pdo-event-store/issues/60)
- Add possibility to send messages to projectors [\#50](https://github.com/prooph/pdo-event-store/issues/50)
- rename projection =\> projector where applicable [\#68](https://github.com/prooph/pdo-event-store/pull/68) ([prolic](https://github.com/prolic))
- Remove PHP\_INT\_MAX constant from interface [\#65](https://github.com/prooph/pdo-event-store/pull/65) ([prolic](https://github.com/prolic))
- Projection manager factory [\#62](https://github.com/prooph/pdo-event-store/pull/62) ([basz](https://github.com/basz))
- Add PDO connection factory [\#61](https://github.com/prooph/pdo-event-store/pull/61) ([prolic](https://github.com/prolic))
- Projection manager, Updates & Bugfixes [\#59](https://github.com/prooph/pdo-event-store/pull/59) ([prolic](https://github.com/prolic))
- fetchStreamNames, fetchCategoryNames, fetchProjectionNames [\#52](https://github.com/prooph/pdo-event-store/pull/52) ([prolic](https://github.com/prolic))
- delete/reset/stop projections [\#51](https://github.com/prooph/pdo-event-store/pull/51) ([prolic](https://github.com/prolic))
- change EventStore::load method [\#47](https://github.com/prooph/pdo-event-store/pull/47) ([prolic](https://github.com/prolic))
- use prepared load statements [\#44](https://github.com/prooph/pdo-event-store/pull/44) ([oqq](https://github.com/oqq))

**Fixed bugs:**

- PersistBlockSize projection option works not properly [\#57](https://github.com/prooph/pdo-event-store/issues/57)
- Improve event store indexes [\#43](https://github.com/prooph/pdo-event-store/issues/43)
- Improve event store indexes [\#66](https://github.com/prooph/pdo-event-store/pull/66) ([prolic](https://github.com/prolic))
- PersistBlockSize projection option works not properly [\#58](https://github.com/prooph/pdo-event-store/pull/58) ([prolic](https://github.com/prolic))
- provides default dsn value for charset in factories [\#56](https://github.com/prooph/pdo-event-store/pull/56) ([oqq](https://github.com/oqq))
- Bugfixes [\#46](https://github.com/prooph/pdo-event-store/pull/46) ([prolic](https://github.com/prolic))

**Closed issues:**

- \[DRAFT\] Death Loop with load reverse lacking $count [\#64](https://github.com/prooph/pdo-event-store/issues/64)
- Performance degrading [\#45](https://github.com/prooph/pdo-event-store/issues/45)
- SingleStreamStrategy should select using generated columns to utilise the indexes [\#42](https://github.com/prooph/pdo-event-store/issues/42)
- MySql EventStore does not support transactions [\#39](https://github.com/prooph/pdo-event-store/issues/39)
- Should resetProjection always be called? [\#38](https://github.com/prooph/pdo-event-store/issues/38)

**Merged pull requests:**

- Fetch method load fix [\#55](https://github.com/prooph/pdo-event-store/pull/55) ([basz](https://github.com/basz))
- implements where and values args on createWhereClauseForMetadata calls [\#54](https://github.com/prooph/pdo-event-store/pull/54) ([basz](https://github.com/basz))
- update to use psr\container [\#53](https://github.com/prooph/pdo-event-store/pull/53) ([basz](https://github.com/basz))
- name should be escaped [\#48](https://github.com/prooph/pdo-event-store/pull/48) ([basz](https://github.com/basz))
- Always call reset projection [\#41](https://github.com/prooph/pdo-event-store/pull/41) ([codeliner](https://github.com/codeliner))
- Run tests using docker [\#40](https://github.com/prooph/pdo-event-store/pull/40) ([codeliner](https://github.com/codeliner))

## [v1.0.0-beta3](https://github.com/prooph/pdo-event-store/tree/v1.0.0-beta3) (2017-01-12)
[Full Changelog](https://github.com/prooph/pdo-event-store/compare/v1.0.0-beta2...v1.0.0-beta3)

## [v1.0.0-beta2](https://github.com/prooph/pdo-event-store/tree/v1.0.0-beta2) (2017-01-12)
[Full Changelog](https://github.com/prooph/pdo-event-store/compare/v1.0.0-beta1...v1.0.0-beta2)

**Implemented enhancements:**

- Projections Update [\#30](https://github.com/prooph/pdo-event-store/pull/30) ([prolic](https://github.com/prolic))

## [v1.0.0-beta1](https://github.com/prooph/pdo-event-store/tree/v1.0.0-beta1) (2017-01-12)
**Implemented enhancements:**

- addStreamToStreamsTable throws wrong exception [\#24](https://github.com/prooph/pdo-event-store/issues/24)
- Add stop\(\) method to query, projection and read model projection [\#10](https://github.com/prooph/pdo-event-store/issues/10)
- UnitTests [\#36](https://github.com/prooph/pdo-event-store/pull/36) ([oqq](https://github.com/oqq))
- MySQL Database Scheme improvements [\#34](https://github.com/prooph/pdo-event-store/pull/34) ([oqq](https://github.com/oqq))
- adds missing phpunit group annotations to container test classes [\#33](https://github.com/prooph/pdo-event-store/pull/33) ([oqq](https://github.com/oqq))
- Convert PostgreSQL payload column from JSONB to JSON. [\#28](https://github.com/prooph/pdo-event-store/pull/28) ([shochdoerfer](https://github.com/shochdoerfer))
- Handle missing tables [\#26](https://github.com/prooph/pdo-event-store/pull/26) ([prolic](https://github.com/prolic))
- Wrap action event emitter [\#23](https://github.com/prooph/pdo-event-store/pull/23) ([prolic](https://github.com/prolic))
- Add convenience methods to event store [\#22](https://github.com/prooph/pdo-event-store/pull/22) ([prolic](https://github.com/prolic))
- add updateStreamMetadata method to event store [\#21](https://github.com/prooph/pdo-event-store/pull/21) ([prolic](https://github.com/prolic))
- update to interop-config 2.0.0 [\#19](https://github.com/prooph/pdo-event-store/pull/19) ([sandrokeil](https://github.com/sandrokeil))
- Persistence strategy [\#18](https://github.com/prooph/pdo-event-store/pull/18) ([prolic](https://github.com/prolic))
- Improvement/interface names [\#15](https://github.com/prooph/pdo-event-store/pull/15) ([basz](https://github.com/basz))
- Updates [\#14](https://github.com/prooph/pdo-event-store/pull/14) ([prolic](https://github.com/prolic))
- Trigger more events [\#13](https://github.com/prooph/pdo-event-store/pull/13) ([prolic](https://github.com/prolic))
- Projections [\#9](https://github.com/prooph/pdo-event-store/pull/9) ([prolic](https://github.com/prolic))
- Update PDO EventStore implemenations [\#8](https://github.com/prooph/pdo-event-store/pull/8) ([prolic](https://github.com/prolic))
- Add postgres support [\#2](https://github.com/prooph/pdo-event-store/pull/2) ([prolic](https://github.com/prolic))
- finally working with mysql [\#1](https://github.com/prooph/pdo-event-store/pull/1) ([prolic](https://github.com/prolic))

**Fixed bugs:**

- addStreamToStreamsTable throws wrong exception [\#24](https://github.com/prooph/pdo-event-store/issues/24)
- Unit test error after update [\#17](https://github.com/prooph/pdo-event-store/issues/17)
- Transaction handling [\#6](https://github.com/prooph/pdo-event-store/issues/6)
- Bug with MySQLSingleStreamStrategy [\#5](https://github.com/prooph/pdo-event-store/issues/5)
- Handle missing tables [\#26](https://github.com/prooph/pdo-event-store/pull/26) ([prolic](https://github.com/prolic))
- fix sprintf usage [\#16](https://github.com/prooph/pdo-event-store/pull/16) ([malukenho](https://github.com/malukenho))
- add tests for stopping projections and queries, fix small bug [\#11](https://github.com/prooph/pdo-event-store/pull/11) ([prolic](https://github.com/prolic))
- Projections [\#9](https://github.com/prooph/pdo-event-store/pull/9) ([prolic](https://github.com/prolic))
- fix stream iterator [\#3](https://github.com/prooph/pdo-event-store/pull/3) ([prolic](https://github.com/prolic))

**Closed issues:**

- What is the reasoning behind using JSONB in PostgreSQL? [\#27](https://github.com/prooph/pdo-event-store/issues/27)
- Repo name [\#12](https://github.com/prooph/pdo-event-store/issues/12)
- Marker Interfaces for indexing strategies [\#4](https://github.com/prooph/pdo-event-store/issues/4)

**Merged pull requests:**

- SQL Query improvements [\#37](https://github.com/prooph/pdo-event-store/pull/37) ([oqq](https://github.com/oqq))
- updates minimum MySQL server version to 5.7.9 in README [\#35](https://github.com/prooph/pdo-event-store/pull/35) ([oqq](https://github.com/oqq))
- Travis config improvement [\#29](https://github.com/prooph/pdo-event-store/pull/29) ([oqq](https://github.com/oqq))
- Factory improvements [\#25](https://github.com/prooph/pdo-event-store/pull/25) ([oqq](https://github.com/oqq))
- Move scripts into separate directories by DB vendor [\#20](https://github.com/prooph/pdo-event-store/pull/20) ([mablae](https://github.com/mablae))
- Work with new event store release [\#7](https://github.com/prooph/pdo-event-store/pull/7) ([malukenho](https://github.com/malukenho))



\* *This Change Log was automatically generated by [github_changelog_generator](https://github.com/skywinder/Github-Changelog-Generator)*
