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

DROP TABLE IF EXISTS `opentsdb_table1`;

CREATE TABLE `opentsdb_table1` (
    `time` timestamp NOT NULL,
    `level_description` string TAG,
    `location` string TAG,
    `value` double,
    timestamp KEY (time)) ENGINE = Analytic WITH (
    enable_ttl = 'false'
);

-- Insert Records:
-- ("2015-08-18T00:00:00Z", "between 6 and 9 feet", "coyote_creek", 8.12),
-- ("2015-08-18T00:00:00Z", "below 3 feet", "santa_monica", 2.064),
-- ("2015-08-18T00:06:00Z", "between 6 and 9 feet", "coyote_creek", 8.005),
-- ("2015-08-18T00:06:00Z", "below 3 feet", "santa_monica", 2.116),
-- ("2015-08-18T00:12:00Z", "between 6 and 9 feet", "coyote_creek", 7.887),
-- ("2015-08-18T00:12:00Z", "below 3 feet", "santa_monica", 2.028);
INSERT INTO opentsdb_table1(time, level_description, location, value)
    VALUES
        (1439827200000, "between 6 and 9 feet", "coyote_creek", 8.12),
        (1439827200000, "below 3 feet", "santa_monica", 2.064),
        (1439827560000, "between 6 and 9 feet", "coyote_creek", 8.005),
        (1439827560000, "below 3 feet", "santa_monica", 2.116),
        (1439827620000, "between 6 and 9 feet", "coyote_creek", 7.887),
        (1439827620000, "below 3 feet", "santa_monica", 2.028);


-- SQLNESS ARG protocol=opentsdb
{
  "start": 1439827200000,
  "end": 1439827620000,
  "queries": [
    {
      "aggregator": "none",
      "metric": "opentsdb_table1",
      "tags": {}
    }
  ]
}
;

-- SQLNESS ARG protocol=opentsdb
{
  "start": 1439827200000,
  "end": 1439827620000,
  "queries": [
    {
      "aggregator": "none",
      "metric": "opentsdb_table1",
      "tags": {
         "location": "coyote_creek"
      }
    }
  ]
}
;

-- SQLNESS ARG protocol=opentsdb
{
  "start": 1439827200000,
  "end": 1439827620000,
  "queries": [
    {
      "aggregator": "sum",
      "metric": "opentsdb_table1",
      "tags": {
      }
    }
  ]
}
;

DROP TABLE IF EXISTS `opentsdb_table1`;
