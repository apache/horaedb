
DROP TABLE IF EXISTS `02_function_aggretate_table1`;

CREATE TABLE `02_function_aggretate_table1` (
    `timestamp` timestamp NOT NULL,
    `arch` string TAG,
    `datacenter` string TAG,
    `value` int,
    timestamp KEY (timestamp)) ENGINE=Analytic
WITH(
	 enable_ttl='false',
	 update_mode='OVERWRITE'
);

INSERT INTO `02_function_aggretate_table1`
    (`timestamp`, `arch`, `datacenter`, `value`)
VALUES
    (1658304762, 'x86-64', 'china', 100),
    (1658304762, 'arm64', 'china', 110);


SELECT sum(`value`) FROM `02_function_aggretate_table1`;
