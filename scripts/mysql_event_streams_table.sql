CREATE TABLE `event_streams` (
  `real_stream_name` VARCHAR(150) COLLATE utf8_bin NOT NULL,
  `stream_name` CHAR(41) COLLATE utf8_bin NOT NULL,
  `metadata` JSON,
  UNIQUE KEY `ix_rsn` (`real_stream_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
