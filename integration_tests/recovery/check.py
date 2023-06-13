#!/usr/bin/env python
# coding: utf-8

import requests
import argparse

api_root = 'http://localhost:5440'
headers = {
    'Content-Type': 'application/json'
}

def get_test_tables(ts):
    table = 'sql_test' + str(ts)
    table2 = 'SQL_TEST' + str(ts)
    return [table, table2]

def get_args():
    parser = argparse.ArgumentParser(description='cmd args')
    parser.add_argument('--timestamp', '-ts', type=int, help='timestamp')
    parser.add_argument('--init_before_check', '-i', help='init_before_check', action="store_true")
    args = vars(parser.parse_args())
    return args


def execute_sql(sql):
    r = requests.post('{}/sql'.format(api_root), json={'query': sql}, headers=headers)
    assert r.status_code == 200, r.text
    return r.json()

def prepare_data(ts, tables):
    for t in tables:
        execute_sql("""
CREATE TABLE if not exists `{}` (
    `t` timestamp NOT NULL,
    `tag1` string TAG,
    `tag2` string TAG,
    `value` double NOT NULL,
    `VALUE2` double NOT NULL,
    timestamp KEY (t)
);
        """.format(t))

    execute_sql("""
insert into {}(t, tag1, tag2, value, VALUE2)
values
({}, "v1", "v2", 1, 2),
({}, "v1", "v2", 11, 22)
    ;
    """.format(tables[0], ts-5000, ts))

    execute_sql("""
insert into {}(t, tag1, tag2, value, VALUE2)
values
({}, "v1", "v2", 10, 20),
({}, "v1", "v2", 110, 220)
    ;
    """.format(tables[1], ts-5000, ts))

def query_and_check(ts, tables):
    expected = {'rows': [{'tsid': 7518337278486593135, 't': ts - 5000, 'tag1': 'v1', 'tag2': 'v2', 'value': 1.0, 'VALUE2': 2.0},\
                         {'tsid': 7518337278486593135, 't': ts, 'tag1': 'v1', 'tag2': 'v2', 'value': 11.0, 'VALUE2': 22.0}]}
    expected2 = {'rows': [{'tsid': 7518337278486593135, 't': ts - 5000, 'tag1': 'v1', 'tag2': 'v2', 'value': 10.0, 'VALUE2': 20.0},\
                          {'tsid': 7518337278486593135, 't': ts, 'tag1': 'v1', 'tag2': 'v2', 'value': 110.0, 'VALUE2': 220.0}]}
    expecteds = [expected, expected2]

    for idx, t in enumerate(tables):
        r = execute_sql("select * from {}".format(t))
        assert r == expecteds[idx]
    
    print('Restart test pass...')

def main():
    args = get_args()
    init_before_check = args['init_before_check']
    ts = args['timestamp']
    test_tables = get_test_tables(args['timestamp'])

    if init_before_check:
        print("Init before check")
        prepare_data(ts, test_tables)
    query_and_check(ts, test_tables)

if __name__ == '__main__':
    main()
