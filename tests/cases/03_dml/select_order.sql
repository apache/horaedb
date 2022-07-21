
DROP TABLE IF EXISTS `03_dml_select_order_table1`;

CREATE TABLE `03_dml_select_order_table1` (
    `timestamp` timestamp NOT NULL,
    `value` int,
    timestamp KEY (timestamp)) ENGINE=Analytic
WITH(
	 enable_ttl='false'
);


INSERT INTO `03_dml_select_order_table1`
VALUES
    (1, 100),
    (2, 1000),
    (3, 200),
    (4, 30000),
    (5, 4400),
    (6, 400);


SELECT
    `timestamp`,
    `value`
FROM
    `03_dml_select_order_table1`
ORDER BY
    `value` ASC;


SELECT
    `timestamp`,
    `value`
FROM
    `03_dml_select_order_table1`
ORDER BY
    `value` DESC;
