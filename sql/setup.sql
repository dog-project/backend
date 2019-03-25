CREATE TABLE weights
(
  id INTEGER PRIMARY KEY,
  lower INTEGER NOT NULL,
  upper INTEGER NOT NULL
);



INSERT INTO weights VALUES (0, 0, 12);
INSERT INTO weights VALUES (1, 13, 25);
INSERT INTO weights VALUES (2, 26, 50);
INSERT INTO weights VALUES (3, 51, 100);
INSERT INTO weights VALUES (4, 100, 500);

CREATE TABLE dogs
(
  id SERIAL PRIMARY KEY,
  submission_time TIMESTAMP NOT NULL DEFAULT NOW(),
  image BYTEA NOT NULL,
  age_months INTEGER NOT NULL,
  weight_id INTEGER REFERENCES weights(id) NOT NULL,
  breed VARCHAR(100) NOT NULL,
  submitter_email VARCHAR(300) NOT NULL
);

CREATE TYPE vote_result AS ENUM ('win', 'loss', 'tie');

CREATE TABLE votes (
  vote_id SERIAL PRIMARY KEY,
  submission_time TIMESTAMP NOT NULL DEFAULT NOW(),

  dog1_id INTEGER REFERENCES dogs(id),
  dog2_id INTEGER REFERENCES dogs(id),
  result vote_result
)