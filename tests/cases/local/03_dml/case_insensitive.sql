
DROP TABLE IF EXISTS case_insensitive_table1;


CREATE TABLE case_insensitive_table1 (
    ts timestamp NOT NULL,
    value1 double,
    timestamp KEY (ts)) ENGINE=Analytic
WITH(
	 enable_ttl='false'
);

INSERT INTO case_insensitive_table1 (ts, value1)
    VALUES (1, 10), (2, 20), (3, 30);


SELECT
    *
FROM
    case_insensitive_table1;

SELECT
    *
FROM
    CASE_INSENSITIVE_TABLE1;


SHOW CREATE TABLE case_insensitive_table1;

SHOW CREATE TABLE CASE_INSENSITIVE_TABLE1;

DESC case_insensitive_table1;

DESC CASE_INSENSITIVE_TABLE1;
