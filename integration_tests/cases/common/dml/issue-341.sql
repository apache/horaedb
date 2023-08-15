
DROP TABLE IF EXISTS `issue341_t1`;
DROP TABLE IF EXISTS `issue341_t2`;

CREATE TABLE `issue341_t1` (
    `timestamp` timestamp NOT NULL,
    `value` int,
    `tag1` string tag,
    timestamp KEY (timestamp)) ENGINE=Analytic
WITH(
	 enable_ttl='false',
	 update_mode='append'
);

INSERT INTO `issue341_t1` (`timestamp`, `value`, `tag1`)
    VALUES (1, 1, "t1"), (2, 2, "t2"), (3, 3, "t3");

SELECT
    `timestamp`,
    `value`
FROM
    `issue341_t1`;

SELECT
    `timestamp`,
    `value`
FROM
    `issue341_t1`
WHERE
    `value` = 3;

-- FilterExec node should not be in plan.
EXPLAIN SELECT
    `timestamp`,
    `value`
FROM
    `issue341_t1`
WHERE
    `value` = 3;

-- FilterExec node should not be in plan.
EXPLAIN SELECT
    `timestamp`,
    `value`
FROM
    `issue341_t1`
WHERE
    tag1 = "t3";

-- Repeat operations above, but with overwrite table

CREATE TABLE `issue341_t2` (
    `timestamp` timestamp NOT NULL,
    `value` double,
    `tag1` string tag,
    timestamp KEY (timestamp)) ENGINE=Analytic
WITH(
	 enable_ttl='false',
	 update_mode='overwrite'
);

INSERT INTO `issue341_t2` (`timestamp`, `value`, `tag1`)
    VALUES (1, 1, "t1"), (2, 2, "t2"), (3, 3, "t3");

SELECT
    `timestamp`,
    `value`
FROM
    `issue341_t2`
WHERE
    `value` = 3;

-- FilterExec node should be in plan.
EXPLAIN SELECT
    `timestamp`,
    `value`
FROM
    `issue341_t2`
WHERE
    `value` = 3;

-- When using tag as filter, FilterExec node should not be in plan.
EXPLAIN SELECT
    `timestamp`,
    `value`
FROM
    `issue341_t2`
WHERE
    tag1 = "t3";

DROP TABLE IF EXISTS `issue341_t1`;
DROP TABLE IF EXISTS `issue341_t2`;
