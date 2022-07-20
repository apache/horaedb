DROP TABLE IF EXISTS 06_show_a;
DROP TABLE IF EXISTS 06_show_b;
DROP TABLE IF EXISTS 06_show_c;

CREATE TABLE 06_show_a (a bigint, b int default 3, c string default 'x', d smallint null, t timestamp NOT NULL, TIMESTAMP KEY(t)) ENGINE = Analytic;
SHOW CREATE TABLE 06_show_a;

CREATE TABLE 06_show_b (a bigint, b int null default null, c string, d smallint null, t timestamp NOT NULL, TIMESTAMP KEY(t)) ENGINE = Analytic;
SHOW CREATE TABLE 06_show_b;

CREATE TABLE 06_show_c (a int, t timestamp NOT NULL, TIMESTAMP KEY(t)) ENGINE = Analytic;
SHOW CREATE TABLE 06_show_c;

DROP TABLE 06_show_a;
DROP TABLE 06_show_b;
DROP TABLE 06_show_c;