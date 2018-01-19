CREATE TABLE `projections` (
  `no` BIGINT(20) NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(150) NOT NULL,
  `position` LONGTEXT,
  `state` LONGTEXT,
  `status` VARCHAR(28) NOT NULL,
  `locked_until` CHAR(26),
  CHECK (`position` IS NULL OR JSON_VALID(`position`)),
  CHECK (`state` IS NULL OR JSON_VALID(`state`)),
  PRIMARY KEY (`no`),
  UNIQUE KEY `ix_name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
