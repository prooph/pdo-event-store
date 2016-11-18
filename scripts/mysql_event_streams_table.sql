CREATE TABLE `event_streams` (
  `no` INT(11) NOT NULL AUTO_INCREMENT,
  `real_stream_name` VARCHAR(150) NOT NULL,
  `stream_name` CHAR(41) NOT NULL,
  `metadata` JSON,
  PRIMARY KEY (`no`),
  UNIQUE KEY `ix_rsn` (`real_stream_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
