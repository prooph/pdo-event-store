CREATE TABLE projections (
  no SERIAL,
  name VARCHAR(150) NOT NULL,
  position JSONB,
  state JSONB,
  locked VARCHAR(150),
  PRIMARY KEY (no),
  UNIQUE (name)
);
