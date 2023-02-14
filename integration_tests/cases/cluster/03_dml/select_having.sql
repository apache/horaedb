DROP TABLE IF EXISTS `03_dml_select_having_table1`;

CREATE TABLE `03_dml_select_having_table1` (
    `timestamp` timestamp NOT NULL,
    `value` int,
    timestamp KEY (timestamp)) ENGINE=Analytic
WITH(
	 enable_ttl='false'
);


INSERT INTO `03_dml_select_having_table1`
    (`timestamp`, `value`)
VALUES
    (1, 101),
    (2, 1002),
    (3, 203),
    (4, 30004),
    (5, 4405),
    (6, 406);


SELECT
    `value` % 3,
    MAX(`value`) AS max
FROM
    `03_dml_select_having_table1`
GROUP BY
    `value` % 3
ORDER BY
    max ASC;


SELECT
    `value` % 3,
    MAX(`value`) AS max
FROM
    `03_dml_select_having_table1`
GROUP BY
    `value` % 3
HAVING
    max > 10000
ORDER BY
    max ASC;

DROP TABLE `03_dml_select_having_table1`;
