#!/usr/bin/env bash

# This only ensure query by mysql protocol is OK,
# Full SQL test in ensured by sqlness tests.
psql -h 127.0.0.1 -p 5433 -e 'show tables'

psql -h 127.0.0.1 -p 5433 -e 'select 1, now();'

psql -h 127.0.0.1 -p 5433 -e 'drop table if exists demo;'

psql -h 127.0.0.1 -p 5433 -e 'CREATE TABLE `demo`(`name`string TAG,`id` int TAG,`value` double NOT NULL,`t` timestamp NOT NULL,TIMESTAMP KEY(t)) ENGINE = Analytic with(enable_ttl=false);'

psql -h 127.0.0.1 -p 5433 -e 'insert into demo (name,value,t)values("ceresdb",1,1691116127622);'

psql -h 127.0.0.1 -p 5433 -e 'select * from demo;'

