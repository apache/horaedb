#!/usr/bin/env python
# coding: utf-8

import requests
import time

api_root = 'http://localhost:5440'
prom_api_root = 'http://localhost:9090'
headers = {
    'Content-Type': 'application/json'
}

def now():
    return int(time.time()) * 1000

table = 'prom_remote_query_test' + str(now())
table2 = 'PROM_REMOTE_QUERY_TEST' + str(now())

def execute_sql(sql):
    r = requests.post('{}/sql'.format(api_root), json={'query': sql}, headers=headers)
    assert r.status_code == 200, r.text

def execute_pql(pql):
    r = requests.get('{}/api/v1/query?query={}'.format(prom_api_root, pql))
    assert r.status_code == 200, r.text
    return r.json()

def prepare_data(ts):
    for t in [table, table2]:
        execute_sql("""
CREATE TABLE if not exists `{}` (
    `t` timestamp NOT NULL,
    `tag1` string TAG,
    `TAG2` string TAG,
    `value` double NOT NULL,
    `VALUE2` double NOT NULL,
    timestamp KEY (t)
);
        """.format(t))

    execute_sql("""
insert into {}(t, tag1, TAG2, value, VALUE2)
values
({}, "v1", "v2", 1, 2),
({}, "v1", "v2", 11, 22)
    ;
    """.format(table, ts-5000, ts))

    execute_sql("""
insert into {}(t, tag1, TAG2, value, VALUE2)
values
({}, "v1", "v2", 10, 20),
({}, "v1", "v2", 110, 220)
    ;
    """.format(table2, ts-5000, ts))


def remote_query(ts):
    ts = ts/1000 # prom return seconds

    r = execute_pql(table + '{tag1="v1"}[5m]')
    result = r['data']['result']
    assert result == [{'metric': {'__name__': table, 'tag1': 'v1', 'TAG2': 'v2'}, 'values': [[ts-5, '1'], [ts, '11']]}]

    r = execute_pql(table + '{TAG2="v2"}[5m]')
    result = r['data']['result']
    assert result == [{'metric': {'__name__': table, 'tag1': 'v1', 'TAG2': 'v2'}, 'values': [[ts-5, '1'], [ts, '11']]}]

    r = execute_pql(table + '{tag1=~"v1"}[5m]')
    result = r['data']['result']
    assert result == [{'metric': {'__name__': table, 'tag1': 'v1', 'TAG2': 'v2'}, 'values': [[ts-5, '1'], [ts, '11']]}]

    r = execute_pql(table + '{tag1!="v1"}[5m]')
    result = r['data']['result']
    assert result == []

    r = execute_pql(table + '{tag1!~"v1"}[5m]')
    result = r['data']['result']
    assert result == []

    # uppercase field
    r = execute_pql(table + '{tag1="v1",__ceresdb_field__="VALUE2"}[5m]')
    result = r['data']['result']
    assert result == [{'metric': {'__name__': table, 'tag1': 'v1', 'TAG2': 'v2'}, 'values': [[ts-5, '2'], [ts, '22']]}]

    # uppercase table
    r = execute_pql(table2 + '{tag1="v1"}[5m]')
    result = r['data']['result']
    assert result == [{'metric': {'__name__': table2, 'tag1': 'v1', 'TAG2': 'v2'}, 'values': [[ts-5, '10'], [ts, '110']]}]

def main():
    ts = now()
    prepare_data(ts)
    remote_query(ts)

if __name__ == '__main__':
    main()
