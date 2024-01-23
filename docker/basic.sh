#!/usr/bin/env bash

set -exo pipefail

trap cleanup EXIT

cleanup() {
    exit_code=$?
    # cat server log when script failed
    if [[ ${exit_code} -ne 0 ]]; then
        docker logs -n 200 ${SERVER_NAME}
    fi
}

ADDR=${HORAEDB_ADDR:-"127.0.0.1"}
PORT=${HORAEDB_PORT:-"5440"}

URL="http://${ADDR}:${PORT}/sql"

function horaedb_query {
    sql=${1}

    curl --location --fail \
         --request POST ${URL} \
         --header 'Content-Type: application/json' \
         --data-raw '{
        "query": "'"${sql}"'"
    }'
}

horaedb_query 'CREATE TABLE `demo` (`name` string TAG, `value` double NOT NULL, `t` timestamp NOT NULL, TIMESTAMP KEY(t)) ENGINE=Analytic with (enable_ttl='\''false'\'')'

horaedb_query 'INSERT INTO demo(t, name, value) VALUES(1651737067000, '\''horaedb'\'', 100)'

horaedb_query 'select * from demo'

horaedb_query 'show create table demo'

horaedb_query 'DROP TABLE demo'
