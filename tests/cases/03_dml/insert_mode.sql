
-- overwrite
DROP TABLE IF EXISTS `03_dml_insert_mode_table1`;

CREATE TABLE `03_dml_insert_mode_table1` (
    `timestamp` timestamp NOT NULL,
    `value` double,
    timestamp KEY (timestamp)) ENGINE=Analytic
WITH(
	 enable_ttl='false',
	 update_mode='OVERWRITE'
);


INSERT INTO `03_dml_insert_mode_table1` (`timestamp`, `value`)
    VALUES (1, 10), (2, 20), (3, 30);

SELECT
    `value`
FROM
    `03_dml_insert_mode_table1`
ORDER BY
    `value` ASC;

INSERT INTO `03_dml_insert_mode_table1` (`timestamp`, `value`)
    VALUES (1, 100), (2, 200), (3, 300);


SELECT
    `value`
FROM
    `03_dml_insert_mode_table1`
ORDER BY
    `value` ASC;


-- append
DROP TABLE IF EXISTS `03_dml_insert_mode_table2`;

CREATE TABLE `03_dml_insert_mode_table2` (
    `timestamp` timestamp NOT NULL,
    `value` double,
    timestamp KEY (timestamp)) ENGINE=Analytic
WITH(
	 enable_ttl='false',
	 update_mode='APPEND'
);


INSERT INTO `03_dml_insert_mode_table2` (`timestamp`, `value`)
    VALUES (1, 10), (2, 20), (3, 30);

SELECT
    `value`
FROM
    `03_dml_insert_mode_table2`
ORDER BY
    `value` ASC;

INSERT INTO `03_dml_insert_mode_table2` (`timestamp`, `value`)
    VALUES (1, 100), (2, 200), (3, 300);

SELECT
    `value`
FROM
    `03_dml_insert_mode_table2`
ORDER BY
    `value` ASC;
