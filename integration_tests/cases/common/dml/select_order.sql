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

DROP TABLE IF EXISTS `03_dml_select_order_table1`;

CREATE TABLE `03_dml_select_order_table1` (
    `timestamp` timestamp NOT NULL,
    `value` int,
    timestamp KEY (timestamp)) ENGINE=Analytic
WITH(
	 enable_ttl='false'
);


INSERT INTO `03_dml_select_order_table1` (`timestamp`, `value`)
VALUES
    (1, 100),
    (2, 1000),
    (3, 200),
    (4, 30000),
    (5, 4400),
    (6, 400);


SELECT
    `timestamp`,
    `value`
FROM
    `03_dml_select_order_table1`
ORDER BY
    `value` ASC;


SELECT
    `timestamp`,
    `value`
FROM
    `03_dml_select_order_table1`
ORDER BY
    `value` DESC;

DROP TABLE `03_dml_select_order_table1`;
