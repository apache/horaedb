--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.
--

-- overwrite
DROP TABLE IF EXISTS `03_dml_insert_mode_table1`;

CREATE TABLE `03_dml_insert_mode_table1` (
    `timestamp` timestamp NOT NULL,
    `value` double,
    `dic` string dictionary,
    timestamp KEY (timestamp)) ENGINE=Analytic
WITH(
	 enable_ttl='false',
	 update_mode='OVERWRITE'
);


INSERT INTO `03_dml_insert_mode_table1` (`timestamp`, `value`, `dic`)
    VALUES (1, +10, "d1"), (2, 0, "d2"), (3, -30, "d1");


SELECT
    *
FROM
    `03_dml_insert_mode_table1`
ORDER BY
    `value` ASC;


INSERT INTO `03_dml_insert_mode_table1` (`timestamp`, `value`)
    VALUES (1, 100), (2, 200), (3, 300);


SELECT
    *
FROM
    `03_dml_insert_mode_table1`
ORDER BY
    `value` ASC;

DROP TABLE `03_dml_insert_mode_table1`;

-- append
DROP TABLE IF EXISTS `03_dml_insert_mode_table2`;

CREATE TABLE `03_dml_insert_mode_table2` (
    `timestamp` timestamp NOT NULL,
    `value` double,
    `dic` string dictionary,
    timestamp KEY (timestamp)) ENGINE=Analytic
WITH(
	 enable_ttl='false',
	 update_mode='APPEND'
);


INSERT INTO `03_dml_insert_mode_table2` (`timestamp`, `value`, `dic`)
    VALUES (1, 10, "d1"), (2, 20, ""), (3, 30, "d2");

SELECT
    *
FROM
    `03_dml_insert_mode_table2`
ORDER BY
    `value` ASC;

INSERT INTO `03_dml_insert_mode_table2` (`timestamp`, `value`, `dic`)
    VALUES (1, 100, "d2"), (2, 200, "d1"), (3, 300, "");

SELECT
    *
FROM
    `03_dml_insert_mode_table2`
ORDER BY
    `value` ASC;

DROP TABLE `03_dml_insert_mode_table2`;

-- default(overwrite)
DROP TABLE IF EXISTS `03_dml_insert_mode_table3`;

CREATE TABLE `03_dml_insert_mode_table3` (
    `timestamp` timestamp NOT NULL,
    `value` double,
    `dic` string dictionary,
    timestamp KEY (timestamp)) ENGINE=Analytic
WITH(
	 enable_ttl='false'
);


INSERT INTO `03_dml_insert_mode_table3` (`timestamp`, `value`, `dic`)
    VALUES (1, 100, "d2"), (2, 200, "d1"), (3, 300, "d1");

-- TODO support insert Null
-- INSERT INTO `03_dml_insert_mode_table3` (`timestamp`, `value`, `dic`) VALUES (1, 100, "d2"), (2, 200, "d1"), (3, 300, Null);

SELECT
    *
FROM
    `03_dml_insert_mode_table3`
ORDER BY
    `value` ASC;

INSERT INTO `03_dml_insert_mode_table3` (`timestamp`, `value`)
    VALUES (1, 100, "d5"), (2, 200, "d6"), (3, 300, "d7");


SELECT
    *
FROM
    `03_dml_insert_mode_table3`
ORDER BY
    `value` ASC;

DROP TABLE `03_dml_insert_mode_table3`;

-- insert with missing columns
DROP TABLE IF EXISTS `03_dml_insert_mode_table4`;

CREATE TABLE `03_dml_insert_mode_table4` (
    `timestamp` timestamp NOT NULL,
    `c1` uint32,
    `c2` string default '123',
    `c3` uint32 default c1 + 1,
    `c4` uint32 default c3 + 1,
    `c5` uint32 default c3 + 10,
    `c6` string default "default",
    timestamp KEY (timestamp)) ENGINE=Analytic
WITH(
	 enable_ttl='false'
);

INSERT INTO `03_dml_insert_mode_table4` (`timestamp`, `c1`, `c5`)
    VALUES (1, 10, 3), (2, 20, 4), (3, 30, 5);

SELECT
    *
FROM
    `03_dml_insert_mode_table4`
ORDER BY
    `c1` ASC;


DROP TABLE IF EXISTS `03_dml_insert_mode_table4`;
