CREATE TABLE `projections` (
  `no` INT(11) NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(150) NOT NULL,
  `position` JSON,
  `state` JSON,
  `locked_until` CHAR(26),
  PRIMARY KEY (`no`),
  UNIQUE KEY `ix_name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
