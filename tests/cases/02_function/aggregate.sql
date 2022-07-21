
DROP TABLE IF EXISTS `02_function_aggretate_table1`;

CREATE TABLE `02_function_aggretate_table1` (
    `timestamp` timestamp NOT NULL,
    `arch` string TAG,
    `datacenter` string TAG,
    `value` int,
    timestamp KEY (timestamp)) ENGINE=Analytic
WITH(
	 enable_ttl='false'
);

INSERT INTO `02_function_aggretate_table1`
    (`timestamp`, `arch`, `datacenter`, `value`)
VALUES
    (1658304762, 'x86-64', 'china', 100),
    (1658304763, 'x86-64', 'china', 200),
    (1658304762, 'arm64', 'china', 110),
    (1658304763, 'arm64', 'china', 210);


SELECT sum(`value`) FROM `02_function_aggretate_table1`;

SELECT
    `arch`,
    sum(`value`)
FROM
    `02_function_aggretate_table1`
WHERE
    `timestamp` BETWEEN 1658304763 AND 1658304763
GROUP BY
    `arch`
ORDER BY
    `arch` DESC;


SELECT count(`value`) FROM `02_function_aggretate_table1`;

SELECT avg(`value`) FROM `02_function_aggretate_table1`;

SELECT max(`value`) FROM `02_function_aggretate_table1`;

SELECT min(`value`) FROM `02_function_aggretate_table1`;


-- duplicate with last insert
INSERT INTO `02_function_aggretate_table1`
    (`timestamp`, `arch`, `datacenter`, `value`)
VALUES
    (1658304762, 'x86-64', 'china', 100);

SELECT count(`value`) FROM `02_function_aggretate_table1`;

SELECT count(distinct(`value`)) FROM `02_function_aggretate_table1`;
