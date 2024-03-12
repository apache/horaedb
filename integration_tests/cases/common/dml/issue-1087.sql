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

CREATE TABLE `issue_1087` (
    `name` string TAG NULL,
    `value` double NOT NULL,
    `t` timestamp NOT NULL,
    timestamp KEY (t))
 ENGINE=Analytic with (enable_ttl='false');


-- Check which optimizer rules we are using now
explain verbose select * from issue_1087;

DROP TABLE `issue_1087`;
