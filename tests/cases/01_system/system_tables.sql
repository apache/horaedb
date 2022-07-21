
DROP TABLE IF EXISTS `01_system_table1`;

CREATE TABLE `01_system_table1` (
    `timestamp` timestamp NOT NULL,
    `arch` string TAG,
    `datacenter` string TAG,
    `hostname` string TAG,
    `value` double,
    timestamp KEY (timestamp)) ENGINE=Analytic;


-- FIXME
SELECT
    `timestamp`,
    `catalog`,
    `schema`,
    `table_name`,
    `engine`
FROM
    system.public.tables
WHERE
    table_name = '01_system_table1';


-- FIXME
SHOW TABLES `name` LIKE '01_system_table1';
