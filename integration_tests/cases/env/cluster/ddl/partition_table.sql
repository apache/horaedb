DROP TABLE IF EXISTS `partition_table_t`;

CREATE TABLE `partition_table_t`(
                                    `name`string TAG,
                                    `id` int TAG,
                                    `value` double NOT NULL,
                                    `t` timestamp NOT NULL,
                                    TIMESTAMP KEY(t)
) PARTITION BY KEY(name) PARTITIONS 4 ENGINE = Analytic with (enable_ttl='false');

SHOW CREATE TABLE partition_table_t;

INSERT INTO partition_table_t (t, name, value)
VALUES (1651737067000, "horaedb0", 100),
       (1651737067000, "horaedb1", 101),
       (1651737067000, "horaedb2", 102),
       (1651737067000, "horaedb3", 103),
       (1651737067000, "horaedb4", 104),
       (1651737067000, "horaedb5", 105),
       (1651737067000, "horaedb6", 106),
       (1651737067000, "horaedb7", 107),
       (1651737067000, "horaedb8", 108),
       (1651737067000, "horaedb9", 109),
       (1651737067000, "horaedb10", 110);

SELECT * from partition_table_t where name = "horaedb0";

SELECT * from partition_table_t where name = "horaedb1";

SELECT * from partition_table_t where name = "horaedb2";

SELECT * from partition_table_t where name = "horaedb3";

SELECT * from partition_table_t where name in ("horaedb0", "horaedb1", "horaedb2", "horaedb3", "horaedb4") order by name;

SELECT * from partition_table_t where name in ("horaedb5", "horaedb6", "horaedb7","horaedb8", "horaedb9", "horaedb10") order by name;

ALTER TABLE partition_table_t ADD COLUMN (b string);

INSERT INTO partition_table_t (t, id, name, value) VALUES (1651737067000, 10, "horaedb0", 100);

ALTER TABLE partition_table_t MODIFY SETTING enable_ttl='true';

SHOW CREATE TABLE __partition_table_t_0;

SHOW CREATE TABLE __partition_table_t_1;

SHOW CREATE TABLE __partition_table_t_2;

SHOW CREATE TABLE __partition_table_t_3;

DROP TABLE IF EXISTS `partition_table_t`;

SHOW CREATE TABLE partition_table_t;

DROP TABLE IF EXISTS `random_partition_table_t`;

CREATE TABLE `random_partition_table_t`(
                                    `name`string TAG,
                                    `id` int TAG,
                                    `value` double NOT NULL,
                                    `t` timestamp NOT NULL,
                                    TIMESTAMP KEY(t)
) PARTITION BY RANDOM PARTITIONS 4 ENGINE = Analytic with (enable_ttl='false', update_mode="APPEND");

SHOW CREATE TABLE random_partition_table_t;

INSERT INTO random_partition_table_t (t, name, value)
VALUES (1651737067000, "horaedb0", 100),
       (1651737067000, "horaedb1", 101),
       (1651737067000, "horaedb2", 102),
       (1651737067000, "horaedb3", 103),
       (1651737067000, "horaedb4", 104),
       (1651737067000, "horaedb5", 105),
       (1651737067000, "horaedb6", 106),
       (1651737067000, "horaedb7", 107),
       (1651737067000, "horaedb8", 108),
       (1651737067000, "horaedb9", 109),
       (1651737067000, "horaedb10", 110);

SELECT * from random_partition_table_t where name = "horaedb0";

SELECT * from random_partition_table_t where name = "horaedb5";

DROP TABLE IF EXISTS `random_partition_table_t`;

SHOW CREATE TABLE random_partition_table_t;

DROP TABLE IF EXISTS `random_partition_table_t_overwrite`;

CREATE TABLE `random_partition_table_t_overwrite`(
                                    `name`string TAG,
                                    `id` int TAG,
                                    `value` double NOT NULL,
                                    `t` timestamp NOT NULL,
                                    TIMESTAMP KEY(t)
) PARTITION BY RANDOM PARTITIONS 4 ENGINE = Analytic with (enable_ttl='false', update_mode="OVERWRITE");
