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


DROP TABLE IF EXISTS `01_system_table1`;

CREATE TABLE `01_system_table1` (
    `timestamp` timestamp NOT NULL,
    `arch` string TAG,
    `datacenter` string TAG,
    `hostname` string TAG,
    `value` double,
    timestamp KEY (timestamp)) ENGINE=Analytic;


-- TODO: when query table in system catalog, it will throw errors now
-- Couldn't find table in table container
-- SELECT
--     `timestamp`,
--     `catalog`,
--     `schema`,
--     `table_name`,
--     `engine`
-- FROM
--     system.public.tables
-- WHERE
--     table_name = '01_system_table1';


-- FIXME
SHOW TABLES LIKE '01%';
