#!/usr/bin/env bash

set +exo pipefail

ADDR=${CERESDB_ADDR:-"127.0.0.1"}
PORT=${CERESDB_PORT:-"5440"}

URL="http://${ADDR}:${PORT}/sql"

# temporarily disable error exit
set -e
for i in `seq 0 30`;
do
    if curl --location ${URL} ; then
        echo "server ready, start to test"
        break
    else
        sleep 1
    fi
done
set +e

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
