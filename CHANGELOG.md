# Change Log

## [v1.10.3](https://github.com/prooph/pdo-event-store/tree/v1.10.3)

[Full Changelog](https://github.com/prooph/pdo-event-store/compare/v1.10.2...v1.10.3)

**Fixed bugs:**

- Issue/Restarting projection during event processing [\#186](https://github.com/prooph/pdo-event-store/pull/186) ([grzegorzstachniukalm](https://github.com/grzegorzstachniukalm))

**Closed issues:**

- Projection implementation dependent on Message interface [\#187](https://github.com/prooph/pdo-event-store/issues/187)

## [v1.10.2](https://github.com/prooph/pdo-event-store/tree/v1.10.2) (2019-01-25)
[Full Changelog](https://github.com/prooph/pdo-event-store/compare/v1.10.1...v1.10.2)

**Fixed bugs:**

- Fix streamCreated condition [\#188](https://github.com/prooph/pdo-event-store/pull/188) ([rodion-k](https://github.com/rodion-k))
- Issue/payload encoding [\#185](https://github.com/prooph/pdo-event-store/pull/185) ([prolic](https://github.com/prolic))

**Merged pull requests:**

- fixed typo into variants documentation [\#184](https://github.com/prooph/pdo-event-store/pull/184) ([AlessandroMinoccheri](https://github.com/AlessandroMinoccheri))

## [v1.10.1](https://github.com/prooph/pdo-event-store/tree/v1.10.1) (2018-11-10)
[Full Changelog](https://github.com/prooph/pdo-event-store/compare/v1.10.0...v1.10.1)

**Fixed bugs:**

- Fix for starting a Projector with `stopping` state, looses stream position state [\#180](https://github.com/prooph/pdo-event-store/pull/180) ([basz](https://github.com/basz))

**Closed issues:**

- Error No such table [\#182](https://github.com/prooph/pdo-event-store/issues/182)
- Projector status [\#181](https://github.com/prooph/pdo-event-store/issues/181)
- Starting a Projector with `stopping` state, looses stream position state [\#179](https://github.com/prooph/pdo-event-store/issues/179)

## [v1.10.0](https://github.com/prooph/pdo-event-store/tree/v1.10.0) (2018-11-03)
[Full Changelog](https://github.com/prooph/pdo-event-store/compare/v1.9.1...v1.10.0)

**Implemented enhancements:**

- Feature/countable stream iterator [\#172](https://github.com/prooph/pdo-event-store/pull/172) ([basz](https://github.com/basz))

**Fixed bugs:**

- deleting running projections will not restart from event 0 [\#168](https://github.com/prooph/pdo-event-store/issues/168)
- MariaDB 10.2.16 causes issues [\#164](https://github.com/prooph/pdo-event-store/issues/164)
- Removed usage of mariadb json\_value with boolean [\#173](https://github.com/prooph/pdo-event-store/pull/173) ([gquemener](https://github.com/gquemener))

**Merged pull requests:**

- Update cs headers [\#178](https://github.com/prooph/pdo-event-store/pull/178) ([basz](https://github.com/basz))
- Implement a second optional MetadataMatcher parameter for fromStream methods [\#177](https://github.com/prooph/pdo-event-store/pull/177) ([fjogeleit](https://github.com/fjogeleit))
- forgotten whilst fixing \#164 [\#174](https://github.com/prooph/pdo-event-store/pull/174) ([basz](https://github.com/basz))

## [v1.9.1](https://github.com/prooph/pdo-event-store/tree/v1.9.1) (2018-09-06)
[Full Changelog](https://github.com/prooph/pdo-event-store/compare/v1.9.0...v1.9.1)

**Fixed bugs:**

- Bugreport/batch size [\#171](https://github.com/prooph/pdo-event-store/pull/171) ([basz](https://github.com/basz))
- Fix/not resetting until all done [\#170](https://github.com/prooph/pdo-event-store/pull/170) ([basz](https://github.com/basz))

**Merged pull requests:**

- apply latest prooph code style trends [\#169](https://github.com/prooph/pdo-event-store/pull/169) ([basz](https://github.com/basz))
- Double bracket typo [\#167](https://github.com/prooph/pdo-event-store/pull/167) ([edwinkortman](https://github.com/edwinkortman))
- Fix create\_event\_stream example links [\#166](https://github.com/prooph/pdo-event-store/pull/166) ([codeliner](https://github.com/codeliner))

## [v1.9.0](https://github.com/prooph/pdo-event-store/tree/v1.9.0) (2018-06-29)
[Full Changelog](https://github.com/prooph/pdo-event-store/compare/v1.9.0-BETA-2...v1.9.0)

## [v1.9.0-BETA-2](https://github.com/prooph/pdo-event-store/tree/v1.9.0-BETA-2) (2018-06-07)
[Full Changelog](https://github.com/prooph/pdo-event-store/compare/v1.9.0-BETA-1...v1.9.0-BETA-2)

**Implemented enhancements:**

- Add message to ConcurrencyException [\#163](https://github.com/prooph/pdo-event-store/pull/163) ([enumag](https://github.com/enumag))
- Check if projection exists before creating it [\#162](https://github.com/prooph/pdo-event-store/pull/162) ([enumag](https://github.com/enumag))

**Closed issues:**

- Confusing ConcurrencyException [\#161](https://github.com/prooph/pdo-event-store/issues/161)
- Ignored error in PdoEventStoreReadModelProjector [\#160](https://github.com/prooph/pdo-event-store/issues/160)

## [v1.9.0-BETA-1](https://github.com/prooph/pdo-event-store/tree/v1.9.0-BETA-1) (2018-06-04)
[Full Changelog](https://github.com/prooph/pdo-event-store/compare/v1.8.4...v1.9.0-BETA-1)

**Implemented enhancements:**

- Postgres event store with schema support [\#157](https://github.com/prooph/pdo-event-store/issues/157)
- Pg schema support [\#159](https://github.com/prooph/pdo-event-store/pull/159) ([ghettovoice](https://github.com/ghettovoice))
- Use MessageConverter in persistence strategies [\#153](https://github.com/prooph/pdo-event-store/pull/153) ([enumag](https://github.com/enumag))

**Closed issues:**

- Charset/collation on event stream tables [\#155](https://github.com/prooph/pdo-event-store/issues/155)

**Merged pull requests:**

- Change MariaDB tables to utf8mb4 [\#158](https://github.com/prooph/pdo-event-store/pull/158) ([darrylhein](https://github.com/darrylhein))
- Change MySQL tables to utf8mb4 [\#156](https://github.com/prooph/pdo-event-store/pull/156) ([darrylhein](https://github.com/darrylhein))

## [v1.8.4](https://github.com/prooph/pdo-event-store/tree/v1.8.4) (2018-05-06)
[Full Changelog](https://github.com/prooph/pdo-event-store/compare/v1.8.3...v1.8.4)

**Fixed bugs:**

- Fix index metadata in MariaDb single stream strategy [\#152](https://github.com/prooph/pdo-event-store/pull/152) ([stepiiik](https://github.com/stepiiik))

## [v1.8.3](https://github.com/prooph/pdo-event-store/tree/v1.8.3) (2018-05-04)
[Full Changelog](https://github.com/prooph/pdo-event-store/compare/v1.8.2...v1.8.3)

**Closed issues:**

- Key value mapping in indexedMetadataFields required [\#150](https://github.com/prooph/pdo-event-store/issues/150)

**Merged pull requests:**

- MariaDB index mapping [\#151](https://github.com/prooph/pdo-event-store/pull/151) ([kochen](https://github.com/kochen))

## [v1.8.2](https://github.com/prooph/pdo-event-store/tree/v1.8.2) (2018-05-03)
[Full Changelog](https://github.com/prooph/pdo-event-store/compare/v1.8.1...v1.8.2)

**Fixed bugs:**

- MariaDB does not use INDEX [\#147](https://github.com/prooph/pdo-event-store/issues/147)
- Fix MariaDB indexed queries [\#149](https://github.com/prooph/pdo-event-store/pull/149) ([kochen](https://github.com/kochen))

**Closed issues:**

- OPTION\_UPDATE\_LOCK\_THRESHOLD does not work with values greater 1000  [\#146](https://github.com/prooph/pdo-event-store/issues/146)
- Upgrade locking mechanism [\#145](https://github.com/prooph/pdo-event-store/issues/145)

## [v1.8.1](https://github.com/prooph/pdo-event-store/tree/v1.8.1) (2018-04-30)
[Full Changelog](https://github.com/prooph/pdo-event-store/compare/v1.8.0...v1.8.1)

**Implemented enhancements:**

- Dispatch PCNTL signal after each event for immediately shutdown [\#144](https://github.com/prooph/pdo-event-store/pull/144) ([sandrokeil](https://github.com/sandrokeil))

**Merged pull requests:**

- Calculate seconds and initialize interval with it [\#148](https://github.com/prooph/pdo-event-store/pull/148) ([codeliner](https://github.com/codeliner))

## [v1.8.0](https://github.com/prooph/pdo-event-store/tree/v1.8.0) (2018-04-29)
[Full Changelog](https://github.com/prooph/pdo-event-store/compare/v1.7.3...v1.8.0)

**Implemented enhancements:**

- Add projection option:  update lock threshold [\#141](https://github.com/prooph/pdo-event-store/pull/141) ([codeliner](https://github.com/codeliner))

**Merged pull requests:**

- Update variants.md [\#143](https://github.com/prooph/pdo-event-store/pull/143) ([Orkin](https://github.com/Orkin))

## [v1.7.3](https://github.com/prooph/pdo-event-store/tree/v1.7.3) (2018-03-26)
[Full Changelog](https://github.com/prooph/pdo-event-store/compare/v1.7.2...v1.7.3)

**Fixed bugs:**

- wrong ProjectionNotFound exception on resetProjection [\#138](https://github.com/prooph/pdo-event-store/issues/138)
- fix multiple calls to reset/stop/delete projection [\#140](https://github.com/prooph/pdo-event-store/pull/140) ([prolic](https://github.com/prolic))

## [v1.7.2](https://github.com/prooph/pdo-event-store/tree/v1.7.2) (2018-03-14)
[Full Changelog](https://github.com/prooph/pdo-event-store/compare/v1.7.1...v1.7.2)

**Fixed bugs:**

- fix OPTION\_LOCK\_TIMEOUT\_MS in PostgresProjectionManager [\#135](https://github.com/prooph/pdo-event-store/pull/135) ([prolic](https://github.com/prolic))

**Merged pull requests:**

- update docs [\#136](https://github.com/prooph/pdo-event-store/pull/136) ([prolic](https://github.com/prolic))

## [v1.7.1](https://github.com/prooph/pdo-event-store/tree/v1.7.1) (2018-02-26)
[Full Changelog](https://github.com/prooph/pdo-event-store/compare/v1.7.0...v1.7.1)

**Fixed bugs:**

- Quote tablenames projections [\#132](https://github.com/prooph/pdo-event-store/pull/132) ([basz](https://github.com/basz))

**Closed issues:**

- problem with json\_decode when payload contains special character \(MySQL\) [\#133](https://github.com/prooph/pdo-event-store/issues/133)
- Missing backticks on table names declarations \(mysql\) [\#128](https://github.com/prooph/pdo-event-store/issues/128)

## [v1.7.0](https://github.com/prooph/pdo-event-store/tree/v1.7.0) (2018-02-06)
[Full Changelog](https://github.com/prooph/pdo-event-store/compare/v1.6.0...v1.7.0)

**Implemented enhancements:**

- upgrade table schemas [\#124](https://github.com/prooph/pdo-event-store/pull/124) ([prolic](https://github.com/prolic))

**Fixed bugs:**

- Always update the projections position [\#125](https://github.com/prooph/pdo-event-store/issues/125)
- Update schemas [\#122](https://github.com/prooph/pdo-event-store/issues/122)
- Quote tablenames [\#130](https://github.com/prooph/pdo-event-store/pull/130) ([basz](https://github.com/basz))
- Always update the projections position [\#126](https://github.com/prooph/pdo-event-store/pull/126) ([hvanoch](https://github.com/hvanoch))
- upgrade table schemas [\#124](https://github.com/prooph/pdo-event-store/pull/124) ([prolic](https://github.com/prolic))

**Closed issues:**

- Ability to replace the DateTimeImmutable class on mocked datetime [\#123](https://github.com/prooph/pdo-event-store/issues/123)

## [v1.6.0](https://github.com/prooph/pdo-event-store/tree/v1.6.0) (2017-12-15)
[Full Changelog](https://github.com/prooph/pdo-event-store/compare/v1.5.2...v1.6.0)

**Implemented enhancements:**

- Use JSON\_UNQUOTE instead of unquoting operator [\#121](https://github.com/prooph/pdo-event-store/pull/121) ([sbacelic](https://github.com/sbacelic))
- test php 7.2 on travis [\#119](https://github.com/prooph/pdo-event-store/pull/119) ([prolic](https://github.com/prolic))

**Fixed bugs:**

- bump mariadb requirements to 10.2.11 [\#120](https://github.com/prooph/pdo-event-store/pull/120) ([prolic](https://github.com/prolic))

**Closed issues:**

- Mariadb eventstore returning "null" aggregate on new mariadb server 10.2.11 [\#116](https://github.com/prooph/pdo-event-store/issues/116)
- Wrong position in projections table [\#114](https://github.com/prooph/pdo-event-store/issues/114)

**Merged pull requests:**

- Fix silently ignored errors in PostgresEventStore [\#118](https://github.com/prooph/pdo-event-store/pull/118) ([enumag](https://github.com/enumag))

## [v1.5.2](https://github.com/prooph/pdo-event-store/tree/v1.5.2) (2017-11-19)
[Full Changelog](https://github.com/prooph/pdo-event-store/compare/v1.5.1...v1.5.2)

**Implemented enhancements:**

- PdoStreamIterator should check json\_last\_error after json\_decode [\#110](https://github.com/prooph/pdo-event-store/issues/110)
- Drop stream table only if it exists [\#108](https://github.com/prooph/pdo-event-store/pull/108) ([sergeyfedotov](https://github.com/sergeyfedotov))

**Fixed bugs:**

- Catch Throwable in transactional\(\) [\#111](https://github.com/prooph/pdo-event-store/pull/111) ([jiripudil](https://github.com/jiripudil))

**Closed issues:**

- Usage of PDO::rowCount\(\) with SELECT statements [\#113](https://github.com/prooph/pdo-event-store/issues/113)
- Add StorageStrategy Benchmarks [\#67](https://github.com/prooph/pdo-event-store/issues/67)

**Merged pull requests:**

- Fixes position in projections [\#115](https://github.com/prooph/pdo-event-store/pull/115) ([Adapik](https://github.com/Adapik))
- PdoStreamIterator should check json\_last\_error after json\_decode [\#112](https://github.com/prooph/pdo-event-store/pull/112) ([denniskoenig](https://github.com/denniskoenig))
- Restructure docs [\#109](https://github.com/prooph/pdo-event-store/pull/109) ([codeliner](https://github.com/codeliner))

## [v1.5.1](https://github.com/prooph/pdo-event-store/tree/v1.5.1) (2017-09-20)
[Full Changelog](https://github.com/prooph/pdo-event-store/compare/v1.5.0...v1.5.1)

**Implemented enhancements:**

- PdoEventStore Interface [\#105](https://github.com/prooph/pdo-event-store/pull/105) ([oqq](https://github.com/oqq))

**Fixed bugs:**

- Aggregate root not found in MariaDb [\#106](https://github.com/prooph/pdo-event-store/issues/106)
- Fix mariadb [\#107](https://github.com/prooph/pdo-event-store/pull/107) ([prolic](https://github.com/prolic))

**Closed issues:**

- Connection preprty never used in PdoStreamIterator [\#103](https://github.com/prooph/pdo-event-store/issues/103)

**Merged pull requests:**

- remove connection property from stream iterator [\#104](https://github.com/prooph/pdo-event-store/pull/104) ([prolic](https://github.com/prolic))

## [v1.5.0](https://github.com/prooph/pdo-event-store/tree/v1.5.0) (2017-07-30)
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
