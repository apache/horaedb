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


DROP TABLE IF EXISTS case_SENSITIVE_table1;


CREATE TABLE case_SENSITIVE_table1 (
    ts timestamp NOT NULL,
    VALUE1 double,
    timestamp KEY (ts)) ENGINE=Analytic
WITH(
	 enable_ttl='false'
);

INSERT INTO case_SENSITIVE_table1 (ts, VALUE1)
    VALUES (1, 10), (2, 20), (3, 30);


SELECT
    *
FROM
    case_SENSITIVE_table1;

SELECT
    *
FROM
    CASE_SENSITIVE_TABLE1;

SELECT
    *
FROM
    `case_SENSITIVE_table1`;

SELECT
    *
FROM
    `CASE_SENSITIVE_TABLE1`;

SHOW CREATE TABLE case_SENSITIVE_table1;

SHOW CREATE TABLE CASE_SENSITIVE_TABLE1;

SHOW CREATE TABLE `case_SENSITIVE_table1`;

SHOW CREATE TABLE `CASE_SENSITIVE_TABLE1`;

DESC case_SENSITIVE_table1;

DESC CASE_SENSITIVE_TABLE1;

DESC `case_SENSITIVE_table1`;

DESC `CASE_SENSITIVE_TABLE1`;

DROP TABLE IF EXISTS case_SENSITIVE_table1;
