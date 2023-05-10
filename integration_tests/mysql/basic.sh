#!/usr/bin/env bash

# This only ensure query by mysql protocol is OK,
# Full SQL test in ensured by sqlness tests.
mysql -h 127.0.0.1 -P 3307 -e 'show tables'

mysql -h 127.0.0.1 -P 3307 -e 'select 1, now()'

mysql -h 127.0.0.1 -P 3307 -e 'CREATE TABLE `demo`(`name`string TAG,`id` int TAG,`value` double NOT NULL,`t` timestamp NOT NULL,TIMESTAMP KEY(t)) ENGINE = Analytic with(enable_ttl=false)'

mysql -h 127.0.0.1 -P 3307 -e 'insert into demo (name,value,t)values("ceresdb",1,1683280523000)'

mysql -h 127.0.0.1 -P 3307 -e 'select * from demo'
