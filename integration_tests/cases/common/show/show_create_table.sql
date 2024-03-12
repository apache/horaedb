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

DROP TABLE IF EXISTS `06_show_a`;
DROP TABLE IF EXISTS `06_show_b`;
DROP TABLE IF EXISTS `06_show_c`;

CREATE TABLE `06_show_a` (a bigint, b int default 3, c string default 'x', d smallint null, t timestamp NOT NULL, TIMESTAMP KEY(t)) ENGINE = Analytic;
SHOW CREATE TABLE `06_show_a`;

CREATE TABLE `06_show_b` (a bigint, b int null default null, c string, d smallint null, t timestamp NOT NULL, TIMESTAMP KEY(t)) ENGINE = Analytic;
SHOW CREATE TABLE `06_show_b`;

CREATE TABLE `06_show_c` (a int, t timestamp NOT NULL, TIMESTAMP KEY(t)) ENGINE = Analytic;
SHOW CREATE TABLE `06_show_c`;

DROP TABLE `06_show_a`;
DROP TABLE `06_show_b`;
DROP TABLE `06_show_c`;