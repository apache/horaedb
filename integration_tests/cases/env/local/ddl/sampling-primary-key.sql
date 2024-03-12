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


DROP TABLE IF EXISTS `sampling_primary_key_table`;

CREATE TABLE `sampling_primary_key_table` (
    v1 double,
    v2 double,
    v3 double,
    v5 double,
    name string TAG,
    myVALUE int64 NOT NULL,
    t timestamp NOT NULL,
    timestamp KEY (t)) ENGINE = Analytic WITH (
    update_mode='append',
    enable_ttl = 'false'
);

show create table `sampling_primary_key_table`;

INSERT INTO `sampling_primary_key_table` (t, name, myVALUE)
    VALUES
    (1695348000000, "horaedb2", 200),
    (1695348000005, "horaedb2", 100),
    (1695348000001, "horaedb1", 100),
    (1695348000003, "horaedb3", 200);

select * from `sampling_primary_key_table`;

-- After flush, its primary key should changed.
-- SQLNESS ARG pre_cmd=flush
show create table `sampling_primary_key_table`;

select * from `sampling_primary_key_table`;

DROP TABLE IF EXISTS `sampling_primary_key_table`;
