#!/usr/bin/env bash

set +exo pipefail

ADDR="127.0.0.1"
PORT="5440"

URL="http://${ADDR}:${PORT}/sql"

curl --location --request POST ${URL} \
     --header 'Content-Type: application/json' \
     --data-raw '{
        "query": "CREATE TABLE `demo` (`name` string TAG, `value` double NOT NULL, `t` timestamp NOT NULL, TIMESTAMP KEY(t)) ENGINE=Analytic with (enable_ttl='\''false'\'')"
    }'

curl --location --request POST ${URL} \
     --header 'Content-Type: application/json' \
     --data-raw '{
        "query": "INSERT INTO demo(t, name, value) VALUES(1651737067000, '\''ceresdb'\'', 100)"
    }'

curl --location --request POST ${URL} \
     --header 'Content-Type: application/json' \
     --data-raw '{
        "query": "select * from demo"
    }'

curl --location --request POST ${URL} \
     --header 'Content-Type: application/json' \
     --data-raw '{
        "query": "show create table demo"
    }'

curl --location --request POST ${URL} \
     --header 'Content-Type: application/json' \
     --data-raw '{
        "query": "DROP TABLE demo"
    }'
