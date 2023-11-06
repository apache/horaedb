
DROP TABLE IF EXISTS `sampling_primary_key_table`;

CREATE TABLE `sampling_primary_key_table` (
    v1 double,
    v2 double,
    v3 double,
    v5 double,
    name string TAG,
    myVALUE int64 NOT NULL,
    t timestamp NOT NULL,
    timestamp KEY (t)) ENGINE = Analytic WITH (
    update_mode='append',
    enable_ttl = 'false'
);

show create table `sampling_primary_key_table`;

INSERT INTO `sampling_primary_key_table` (t, name, myVALUE)
    VALUES
    (1695348000000, "ceresdb2", 200),
    (1695348000005, "ceresdb2", 100),
    (1695348000001, "ceresdb1", 100),
    (1695348000003, "ceresdb3", 200);

select * from `sampling_primary_key_table`;

-- After flush, its primary key should changed.
-- SQLNESS ARG pre_cmd=flush
show create table `sampling_primary_key_table`;

select * from `sampling_primary_key_table`;

DROP TABLE IF EXISTS `sampling_primary_key_table`;
