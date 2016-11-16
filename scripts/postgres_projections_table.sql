CREATE TABLE projections (
  no SERIAL,
  name VARCHAR(150) NOT NULL,
  position JSONB,
  state JSONB,
  PRIMARY KEY (no),
  UNIQUE (name)
);
