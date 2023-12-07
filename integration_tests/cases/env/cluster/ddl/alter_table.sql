DROP TABLE IF EXISTS `05_alter_table_t0`;

CREATE TABLE `05_alter_table_t0`(a int, t timestamp NOT NULL, dic string dictionary, TIMESTAMP KEY(t)) ENGINE = Analytic with (enable_ttl='false');
INSERT INTO TABLE `05_alter_table_t0`(a, t, dic) values(1, 1 , "d1");
SELECT * FROM `05_alter_table_t0`;

-- doesn't support rename
ALTER TABLE `05_alter_table_t0` RENAME TO `t1`;

ALTER TABLE `05_alter_table_t0` add COLUMN (b string);
DESCRIBE TABLE `05_alter_table_t0`;
INSERT INTO TABLE `05_alter_table_t0`(a, b, t, dic) values (2, '2', 2, "d2");
SELECT * FROM `05_alter_table_t0`;

-- doesn't support drop column
ALTER TABLE `05_alter_table_t0` DROP COLUMN b;
DESCRIBE TABLE `05_alter_table_t0`;
SELECT * FROM `05_alter_table_t0`;

DROP TABLE `05_alter_table_t0`;

-- alter table options
CREATE TABLE `05_alter_table_t1`(a int, t timestamp NOT NULL, TIMESTAMP KEY(t)) ENGINE = Analytic;
ALTER TABLE `05_alter_table_t1` MODIFY SETTING write_buffer_size='300M';

show create table;
drop table 05_alter_table_t1;

CREATE TABLE `05_alter_table_t1` (`sid` uint64 NOT NULL, `t` timestamp NOT NULL, `a` int, PRIMARY KEY(tsid,t), TIMESTAMP KEY(t)) ENGINE=Analytic WITH(arena_block_size='2097152', compaction_strategy='default', compression='ZSTD', enable_ttl='true', num_rows_per_row_group='8192', segment_duration='', storage_format='AUTO', ttl='7d', update_mode='OVERWRITE', write_buffer_size='314572800');
ALTER TABLE `05_alter_table_t1` MODIFY SETTING ttl='10d';
show create table;
drop table 05_alter_table_t1;


