DROP TABLE IF EXISTS `02_function_date_bin_table`;

CREATE TABLE `02_function_date_bin_table` (
  `timestamp` timestamp NOT NULL,
  `value` int,
  timestamp KEY (timestamp)) ENGINE=Analytic
WITH(
	 enable_ttl='false'
);

INSERT INTO `02_function_date_bin_table`
(`timestamp`, `value`)
VALUES
    (1659577423000, 1),
    (1659577422000, 2),
    (1659577320000, 3),
    (1659571200000, 4),
    (1659484800000, 5),
    (1656777600000, 6);

SELECT `timestamp`, DATE_BIN(INTERVAL '30' second, `timestamp`, TIMESTAMP '2001-01-01T00:00:00Z') as `time` FROM `02_function_date_bin_table` order by `timestamp`;
SELECT `timestamp`, DATE_BIN(INTERVAL '15' minute, `timestamp`, TIMESTAMP '2001-01-01T00:00:00Z') as `time` FROM `02_function_date_bin_table` order by `timestamp`;
SELECT `timestamp`, DATE_BIN(INTERVAL '2' hour, `timestamp`, TIMESTAMP '2001-01-01T00:00:00Z') as `time` FROM `02_function_date_bin_table` order by `timestamp`;

DROP TABLE `02_function_date_bin_table`;
