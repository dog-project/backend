CREATE TABLE dogs
(
  id    SERIAL PRIMARY KEY,
  image BYTEA,
  --   age
  breed VARCHAR(100)
);

CREATE TABLE submissions
(
  id              SERIAL PRIMARY KEY,
  email           VARCHAR(300),
  submission_time TIMESTAMP,
  dog_id          INTEGER,
  FOREIGN KEY (dog_id) REFERENCES dogs (id)
);
