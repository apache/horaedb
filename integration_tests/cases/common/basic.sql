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

DROP TABLE IF EXISTS `demo`;

CREATE TABLE demo (
    name string TAG,
    value double NOT NULL,
    t timestamp NOT NULL,
    timestamp KEY (t)) ENGINE = Analytic WITH (
    enable_ttl = 'false'
);


INSERT INTO demo (t, name, value)
    VALUES (1651737067000, 'horaedb', 100);


SELECT * FROM demo;

INSERT INTO demo (t, name, value)
    VALUES (1651737067001, "horaedb", 100);

SELECT * FROM demo;

DROP TABLE IF EXISTS `demo`;

CREATE TABLE `DeMo` (
    `nAmE` string TAG,
    value double NOT NULL,
    t timestamp NOT NULL,
    timestamp KEY (t)) ENGINE = Analytic WITH (
    enable_ttl = 'false'
);


SELECT `nAmE` FROM `DeMo`;

DROP TABLE `DeMo`;

DROP TABLE IF EXISTS `binary_demo`;

CREATE TABLE `binary_demo` (
    `name` string TAG,
    `value` varbinary NOT NULL,
    `t` timestamp NOT NULL,
    timestamp KEY (t)) ENGINE=Analytic WITH (
    enable_ttl = 'false'
);

INSERT INTO binary_demo(t, name, value) VALUES(1667374200022, 'horaedb', x'11');

SELECT * FROM binary_demo WHERE value = x'11';

DROP TABLE `binary_demo`;
