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

DROP TABLE IF EXISTS `03_dml_select_having_table1`;

CREATE TABLE `03_dml_select_having_table1` (
    `timestamp` timestamp NOT NULL,
    `value` int,
    timestamp KEY (timestamp)) ENGINE=Analytic
WITH(
	 enable_ttl='false'
);


INSERT INTO `03_dml_select_having_table1`
    (`timestamp`, `value`)
VALUES
    (1, 101),
    (2, 1002),
    (3, 203),
    (4, 30004),
    (5, 4405),
    (6, 406);


SELECT
    `value` % 3,
    MAX(`value`) AS max
FROM
    `03_dml_select_having_table1`
GROUP BY
    `value` % 3
ORDER BY
    max ASC;


SELECT
    `value` % 3,
    MAX(`value`) AS max
FROM
    `03_dml_select_having_table1`
GROUP BY
    `value` % 3
HAVING
    max > 10000
ORDER BY
    max ASC;

DROP TABLE `03_dml_select_having_table1`;
