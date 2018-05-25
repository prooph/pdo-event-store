CREATE SCHEMA IF NOT EXISTS prooph;

CREATE TABLE prooph.event_streams (
  no BIGSERIAL,
  real_stream_name VARCHAR(150) NOT NULL,
  stream_name CHAR(150) NOT NULL,
  metadata JSONB,
  category VARCHAR(150),
  PRIMARY KEY (no),
  UNIQUE (stream_name)
);
CREATE INDEX on prooph.event_streams (category);
