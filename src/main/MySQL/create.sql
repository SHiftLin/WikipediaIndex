CREATE TABLE wikiindex
(
  word   CHAR(255) NOT NULL
    PRIMARY KEY,
  start  BIGINT    NOT NULL,
  length BIGINT    NOT NULL,
  count  BIGINT    NOT NULL
);

