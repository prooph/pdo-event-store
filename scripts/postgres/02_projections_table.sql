CREATE TABLE projections (
  no BIGSERIAL,
  name VARCHAR(150) NOT NULL,
  position JSONB,
  state JSONB,
  locked_until CHAR(26),
  PRIMARY KEY (no),
  UNIQUE (name)
);
