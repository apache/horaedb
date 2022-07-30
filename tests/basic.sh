#!/usr/bin/env bash

set +exo pipefail

ADDR=${CERESDB_ADDR:-"127.0.0.1"}
PORT=${CERESDB_PORT:-"5440"}

URL="http://${ADDR}:${PORT}/sql"

function ceresdb_query {
    sql=${1}

    curl --location --fail-with-body \
         --request POST ${URL} \
         --header 'Content-Type: application/json' \
         --data-raw '{
        "query": "'${sql}'"
    }'
}

ceresdb_query 'CREATE TABLE `demo` (`name` string TAG, `value` double NOT NULL, `t` timestamp NOT NULL, TIMESTAMP KEY(t)) ENGINE=Analytic with (enable_ttl='\''false'\'')'

ceresdb_query 'INSERT INTO demo(t, name, value) VALUES(1651737067000, '\''ceresdb'\'', 100)'

ceresdb_query 'select * from demo'

ceresdb_query 'show create table demo'

ceresdb_query 'DROP TABLE demo'
