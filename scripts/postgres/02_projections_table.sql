CREATE TABLE projections (
  no SERIAL,
  name VARCHAR(150) NOT NULL,
  position JSONB,
  state JSONB,
  locked_until CHAR(26),
  PRIMARY KEY (no),
  UNIQUE (name)
);
