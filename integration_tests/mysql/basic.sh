#!/usr/bin/env bash

# This only ensure query by mysql protocol is OK,
# Full SQL test in ensured by sqlness tests.
mysql -h 127.0.0.1 -P 3307 -e 'show tables'

mysql -h 127.0.0.1 -P 3307 -e 'select 1, now()'
