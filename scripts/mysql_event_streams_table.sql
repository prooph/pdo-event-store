CREATE TABLE `event_streams` (
  `real_stream_name` VARCHAR(150) COLLATE utf8_unicode_ci NOT NULL,
  `stream_name` CHAR(40) COLLATE utf8_bin NOT NULL,
  `metadata` JSON,
  UNIQUE KEY `ix_rsn` (`real_stream_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
