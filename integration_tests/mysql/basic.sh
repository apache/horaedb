#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


# This only ensure query by mysql protocol is OK,
# Full SQL test in ensured by sqlness tests.
mysql -h 127.0.0.1 -P 3307 -e 'show tables'

mysql -h 127.0.0.1 -P 3307 -e 'select 1, now()'

mysql -h 127.0.0.1 -P 3307 -e 'CREATE TABLE `demo`(`name`string TAG,`id` int TAG,`value` double NOT NULL,`t` timestamp NOT NULL,TIMESTAMP KEY(t)) ENGINE = Analytic with(enable_ttl=false)'

mysql -h 127.0.0.1 -P 3307 -e 'insert into demo (name,value,t)values("horaedb",1,1683280523000)'

mysql -h 127.0.0.1 -P 3307 -e 'select * from demo'
