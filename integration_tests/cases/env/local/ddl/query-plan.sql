DROP TABLE IF EXISTS `03_dml_select_real_time_range`;
DROP TABLE IF EXISTS `03_append_mode_table`;

CREATE TABLE `03_dml_select_real_time_range` (
    name string TAG,
    value double NOT NULL,
    t timestamp NOT NULL,
    timestamp KEY (t)) ENGINE = Analytic WITH (
    enable_ttl = 'false',
    segment_duration='2h'
);

INSERT INTO `03_dml_select_real_time_range` (t, name, value)
    VALUES
    (1695348000000, "horaedb", 100),
    (1695348001000, "horaedb", 200),
    (1695348002000, "horaedb", 300);

-- This query should include memtable
-- SQLNESS REPLACE duration=\d+.?\d*(µ|m|n) duration=xx
-- SQLNESS REPLACE metrics=\[.*?s\] metrics=xx
explain analyze select t from `03_dml_select_real_time_range`
where t > 1695348001000;

-- This query should have higher priority
-- SQLNESS REPLACE duration=\d+.?\d*(µ|m|n) duration=xx
-- SQLNESS REPLACE metrics=\[.*?s\] metrics=xx
explain analyze select t from `03_dml_select_real_time_range`
where t >= 1695348001000 and t < 1695348002000;

-- This query should have higher priority
-- SQLNESS REPLACE duration=\d+.?\d*(µ|m|n) duration=xx
-- SQLNESS REPLACE metrics=\[.*?s\] metrics=xx
explain analyze select name from `03_dml_select_real_time_range`
where t >= 1695348001000 and t < 1695348002000;

-- This query should not include memtable
-- SQLNESS REPLACE duration=\d+.?\d*(µ|m|n) duration=xx
-- SQLNESS REPLACE metrics=\[.*?s\] metrics=xx
explain analyze select t from `03_dml_select_real_time_range`
where t > 1695348002000;

-- SQLNESS ARG pre_cmd=flush
-- SQLNESS REPLACE duration=\d+.?\d*(µ|m|n) duration=xx
-- SQLNESS REPLACE project_record_batch=\d+.?\d*(µ|m|n) project_record_batch=xx
-- SQLNESS REPLACE metrics=\[.*?s\] metrics=xx
-- This query should include SST
explain analyze select t from `03_dml_select_real_time_range`
where t > 1695348001000;

-- This query should not include SST
-- SQLNESS REPLACE metrics=\[.*?s\] metrics=xx
explain analyze select t from `03_dml_select_real_time_range`
where t > 1695348002000;

-- Table with an 'append' update mode
CREATE TABLE `03_append_mode_table` (
    name string TAG,
    value double NOT NULL,
    t timestamp NOT NULL,
    timestamp KEY (t)) ENGINE = Analytic WITH (
    enable_ttl = 'false',
    segment_duration = '2h',
    update_mode = 'append'
);

INSERT INTO `03_append_mode_table` (t, name, value)
    VALUES
    (1695348000000, "ceresdb", 100),
    (1695348001000, "ceresdb", 200),
    (1695348002000, "ceresdb", 300);

-- Should just fetch projected columns from memtable
-- SQLNESS REPLACE duration=\d+.?\d*(µ|m|n) duration=xx
-- SQLNESS REPLACE since_create=\d+.?\d*(µ|m|n) since_create=xx
-- SQLNESS REPLACE since_init=\d+.?\d*(µ|m|n) since_init=xx
-- SQLNESS REPLACE elapsed_compute=\d+.?\d*(µ|m|n) elapsed_compute=xx
-- SQLNESS REPLACE metrics=\[.*?s\] metrics=xx
explain analyze select t from `03_append_mode_table`
where t >= 1695348001000 and name = 'ceresdb';

-- Should just fetch projected columns from SST
-- SQLNESS ARG pre_cmd=flush
-- SQLNESS REPLACE duration=\d+.?\d*(µ|m|n) duration=xx
-- SQLNESS REPLACE since_create=\d+.?\d*(µ|m|n) since_create=xx
-- SQLNESS REPLACE since_init=\d+.?\d*(µ|m|n) since_init=xx
-- SQLNESS REPLACE elapsed_compute=\d+.?\d*(µ|m|n) elapsed_compute=xx
-- SQLNESS REPLACE project_record_batch=\d+.?\d*(µ|m|n) project_record_batch=xx
-- SQLNESS REPLACE metrics=\[.*?s\] metrics=xx
explain analyze select t from `03_append_mode_table`
where t >= 1695348001000 and name = 'ceresdb';

CREATE TABLE `TEST_QUERY_PRIORITY` (
    NAME string TAG,
    VALUE double NOT NULL,
    TS timestamp NOT NULL,
    timestamp KEY (TS)) ENGINE = Analytic WITH (
    enable_ttl = 'false',
    segment_duration = '2h',
    update_mode = 'append'
);

-- This query should have higher priority
-- SQLNESS REPLACE duration=\d+.?\d*(µ|m|n) duration=xx
-- SQLNESS REPLACE metrics=\[.*?s\] metrics=xx
explain analyze select TS from `TEST_QUERY_PRIORITY`
where TS >= 1695348001000 and TS < 1695348002000;

-- This query should have higher priority
-- SQLNESS REPLACE duration=\d+.?\d*(µ|m|n) duration=xx
-- SQLNESS REPLACE metrics=\[.*?s\] metrics=xx
explain analyze select TS from `TEST_QUERY_PRIORITY`
where TS >= 1695348001000;

DROP TABLE `03_dml_select_real_time_range`;
DROP TABLE `03_append_mode_table`;
DROP TABLE `TEST_QUERY_PRIORITY`;
