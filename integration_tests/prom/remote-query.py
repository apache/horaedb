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

def execute_sql(sql):
    r = requests.post('{}/sql'.format(api_root), json={'query': sql}, headers=headers)
    assert r.status_code == 200, r.text

def execute_pql(pql):
    r = requests.get('{}/api/v1/query?query={}'.format(prom_api_root, pql))
    assert r.status_code == 200, r.text
    return r.json()

def prepare_data(ts):
    execute_sql("""
CREATE TABLE if not exists `{}` (
    `t` timestamp NOT NULL,
    `tag1` string TAG,
    `tag2` string TAG,
    `value` double NOT NULL,
    `value2` double NOT NULL,
    timestamp KEY (t)
);
    """.format(table))

    execute_sql("""
insert into {}(t, tag1, tag2, value, value2)
values
({}, "v1", "v2", 1, 2),
({}, "v1", "v2", 11, 22)
    ;
    """.format(table, ts-5000, ts))


def remote_query(ts):
    ts = ts/1000 # prom return seconds

    r = execute_pql(table + '{tag1="v1"}[5m]')
    result = r['data']['result']
    assert result == [{'metric': {'__name__': table, 'tag1': 'v1', 'tag2': 'v2'}, 'values': [[ts-5, '1'], [ts, '11']]}]

    r = execute_pql(table + '{tag1=~"v1"}[5m]')
    result = r['data']['result']
    assert result == [{'metric': {'__name__': table, 'tag1': 'v1', 'tag2': 'v2'}, 'values': [[ts-5, '1'], [ts, '11']]}]

    r = execute_pql(table + '{tag1!="v1"}[5m]')
    result = r['data']['result']
    assert result == []

    r = execute_pql(table + '{tag1!~"v1"}[5m]')
    result = r['data']['result']
    assert result == []

def main():
    ts = now()
    prepare_data(ts)
    remote_query(ts)

if __name__ == '__main__':
    main()
