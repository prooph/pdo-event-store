CREATE TABLE projections (
  no BIGSERIAL,
  name VARCHAR(150) NOT NULL,
  position JSONB,
  state JSONB,
  status VARCHAR(28) NOT NULL,
  locked_until CHAR(26),
  PRIMARY KEY (no),
  UNIQUE (name)
);
