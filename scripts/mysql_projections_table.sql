CREATE TABLE `projections` (
  `no` INT(11) NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(150) COLLATE utf8_bin NOT NULL,
  `position` JSON,
  `state` JSON,
  PRIMARY KEY (`no`),
  INDEX `ix_name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
