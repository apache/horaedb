DROP TABLE IF EXISTS 05_alter_table_t0;

CREATE TABLE 05_alter_table_t0(a int, t timestamp NOT NULL, TIMESTAMP KEY(t)) ENGINE = Analytic with (enable_ttl='false');
INSERT INTO TABLE 05_alter_table_t0(a, t) values(1,1);
SELECT * FROM 05_alter_table_t0;

-- doesn't support rename
ALTER TABLE 05_alter_table_t0 RENAME TO `t1`;

ALTER TABLE 05_alter_table_t0 add COLUMN (b string);
DESCRIBE TABLE 05_alter_table_t0;
INSERT INTO TABLE 05_alter_table_t0(a, b, t) values (2, '2', 2);
SELECT * FROM 05_alter_table_t0;

-- doesn't support drop column
ALTER TABLE 05_alter_table_t0 DROP COLUMN b;
DESCRIBE TABLE 05_alter_table_t0;
SELECT * FROM 05_alter_table_t0;

DROP TABLE 05_alter_table_t0;