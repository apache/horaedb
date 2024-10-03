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

DROP TABLE IF EXISTS `compact_table1`;

CREATE TABLE `compact_table1` (
    `timestamp` timestamp NOT NULL,
    `value` double,
    `dic` string dictionary,
    timestamp KEY (timestamp)) ENGINE=Analytic
WITH(
	 enable_ttl='false',
     update_mode='OVERWRITE'
);


INSERT INTO `compact_table1` (`timestamp`, `value`, `dic`)
    VALUES (1, 100, "d1"), (2, 200, "d2"), (3, 300, "d3");

-- SQLNESS ARG pre_cmd=flush
INSERT INTO `compact_table1` (`timestamp`, `value`, `dic`)
    VALUES (1, 100, "update_d1"), (2, 200, "update_d2"), (3, 300, "update_d3");

-- SQLNESS ARG pre_cmd=flush
INSERT INTO `compact_table1` (`timestamp`, `value`, `dic`)
    VALUES (4, 400, "d4"), (5, 500, "d5"), (6, 600, "d6");

-- SQLNESS ARG pre_cmd=flush
INSERT INTO `compact_table1` (`timestamp`, `value`, `dic`)
    VALUES (4, 400, "update_d4"), (5, 500, "update_d5"), (6, 600, "update_d6");

-- SQLNESS ARG pre_cmd=flush
INSERT INTO `compact_table1` (`timestamp`, `value`, `dic`)
    VALUES (7, 700, "d7"), (8, 800, "d8"), (9, 900, "d9");

-- SQLNESS ARG pre_cmd=flush
INSERT INTO `compact_table1` (`timestamp`, `value`, `dic`)
    VALUES (7, 700, "update_d7"), (8, 800, "update_d8"), (9, 900, "update_d9");

-- SQLNESS ARG pre_cmd=flush
INSERT INTO `compact_table1` (`timestamp`, `value`, `dic`)
    VALUES (10, 1000, "d10"), (11, 1100, "d11"), (12, 1200, "d12");

-- SQLNESS ARG pre_cmd=flush
INSERT INTO `compact_table1` (`timestamp`, `value`, `dic`)
    VALUES (10, 1000, "update_d10"), (11, 1100, "update_d11"), (12, 1200, "update_d12");


-- trigger manual compaction after flush memtable
-- SQLNESS ARG pre_cmd=flush
-- SQLNESS ARG pre_cmd=compact
SELECT
    *
FROM
    `compact_table1`
ORDER BY
    `value` ASC;


DROP TABLE `compact_table1`;
