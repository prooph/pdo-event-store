CREATE TABLE event_streams (
  real_stream_name VARCHAR(150) NOT NULL,
  stream_name CHAR(41) NOT NULL,
  metadata JSONB,
  UNIQUE (stream_name)
);
