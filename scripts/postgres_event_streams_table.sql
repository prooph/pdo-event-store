CREATE TABLE event_streams (
  no SERIAL,
  real_stream_name VARCHAR(150) NOT NULL,
  stream_name CHAR(41) NOT NULL,
  metadata JSONB,
  PRIMARY KEY (no),
  UNIQUE (stream_name)
);
