DROP TABLE IF EXISTS `03_dml_select_real_time_range`;

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
    (1695348000000, "ceresdb", 100),
    (1695348001000, "ceresdb", 200),
    (1695348002000, "ceresdb", 300);

-- This query should include memtable
-- SQLNESS REPLACE duration=\d+.?\d*(µ|m|n) duration=xx
explain analyze select t from `03_dml_select_real_time_range`
where t > 1695348001000;

-- This query should not include memtable
-- SQLNESS REPLACE duration=\d+.?\d*(µ|m|n) duration=xx
explain analyze select t from `03_dml_select_real_time_range`
where t > 1695348002000;

-- SQLNESS ARG pre_cmd=flush
-- SQLNESS REPLACE duration=\d+.\d*(µ|m) duration=xx
-- SQLNESS REPLACE project_record_batch=\d+.\d*(µ|m) project_record_batch=xx
-- This query should include SST
explain analyze select t from `03_dml_select_real_time_range`
where t > 1695348001000;

-- This query should not include SST
explain analyze select t from `03_dml_select_real_time_range`
where t > 1695348002000;

DROP TABLE `03_dml_select_real_time_range`;
