#!/usr/bin/env python
# coding: utf-8

import requests
import time

api_root = 'http://localhost:5440'
headers = {
    'Content-Type': 'application/json'
}
table_prefix = 'opentsdb_test_'
table2_prefix = 'opentsdb_test2_'


def now():
    return int(time.time()) * 1000


def execute_sql(sql):
    r = requests.post('{}/sql'.format(api_root), json={'query': sql}, headers=headers)
    return r


def drop_table(table_name):
    sql = """
DROP TABLE IF EXISTS {}
    """.format(table_name)
    r = execute_sql(sql)
    assert r.status_code == 200, r.text


def show_create_table(table_name):
    sql = """
SHOW CREATE TABLE {}    
    """.format(table_name)
    r = execute_sql(sql)
    assert r.status_code == 200
    return r


def execute_sql_query(sql):
    r = execute_sql(sql)
    assert r.status_code == 200
    return r


def execute_put(points):
    r = requests.post('{}/opentsdb/api/put'.format(api_root), data=points)
    return r


def execute_put_then_assert_fail(points):
    r = execute_put(points)
    assert r.status_code == 500


def execute_put_then_assert_success(points):
    r = execute_put(points)
    assert r.status_code == 204


def test_put_validate_error():
    ts = now();
    # empty json string
    execute_put_then_assert_fail("")

    # invalid json
    execute_put_then_assert_fail("{xxx")

    # empty metric
    execute_put_then_assert_fail("""
{
    "metric": "",
    "timestamp": {ts},
    "value": 18,
    "tags": {
        "host": "web01",
        "dc": "lga"
    }
}
    """.replace('{ts}', str(ts)))

    # empty tag
    execute_put_then_assert_fail("""
{
    "metric": "sys.cpu.nice",
    "timestamp": {ts},
    "value": 18,
    "tags": {
    }
}
    """.replace('{ts}', str(ts)))

    # empty tag name
    execute_put_then_assert_fail("""
{
    "metric": "sys.cpu.nice",
    "timestamp": {ts},
    "value": 18,
    "tags": {
        "": "web01",
        "dc": "lga"
    }
}
    """.replace('{ts}', str(ts)))

    # too small timestamp
    execute_put_then_assert_fail("""
{
    "metric": "sys.cpu.nice",
    "timestamp": 1,
    "value": 18,
    "tags": {
        "host": "web01",
        "dc": "lga"
    }
}
    """.replace('{ts}', str(ts)))

    # too big timestamp
    execute_put_then_assert_fail("""
{
    "metric": "sys.cpu.nice",
    "timestamp": 10000000000000,
    "value": 18,
    "tags": {
        "host": "web01",
        "dc": "lga"
    }
}
    """.replace('{ts}', str(ts)))


def test_put_single_point_with_int_value():
    ts = now()
    table_name = table_prefix + str(ts)
    drop_table(table_name)

    execute_put_then_assert_success("""
{
    "metric": "{metric}",
    "timestamp": {ts},
    "value": 9527,
    "tags": {
        "host": "web01",
        "dc": "lga"
    }
}
    """.replace('{metric}', table_name).replace('{ts}', str(ts)))

    r = show_create_table(table_name)
    assert r.text.__contains__('`tsid` uint64 NOT NULL')
    assert r.text.__contains__('`timestamp` timestamp NOT NULL')
    assert r.text.__contains__('`dc` string TAG')
    assert r.text.__contains__('`host` string TAG')
    # value is a double column
    assert r.text.__contains__('`value` double')

    r = execute_sql_query("""
SELECT timestamp, dc, host, value FROM {metric}
    """.replace('{metric}', table_name))
    assert r.text == """{"rows":[{"timestamp":{ts},"dc":"lga","host":"web01","value":9527.0}]}""".strip().replace('{ts}', str(ts))


def test_put_single_point_with_float_value():
    ts = now()
    table_name = table_prefix + str(ts)
    drop_table(table_name)

    execute_put_then_assert_success("""
{
    "metric": "{metric}",
    "timestamp": {ts},
    "value": 95.27,
    "tags": {
        "host": "web01",
        "dc": "lga"
    }
}
    """.replace('{metric}', table_name).replace('{ts}', str(ts)))

    r = show_create_table(table_name)
    assert r.text.__contains__('`tsid` uint64 NOT NULL')
    assert r.text.__contains__('`timestamp` timestamp NOT NULL')
    assert r.text.__contains__('`dc` string TAG')
    assert r.text.__contains__('`host` string TAG')
    # value is a double column
    assert r.text.__contains__('`value` double')

    r = execute_sql_query("""
SELECT timestamp, dc, host, value FROM {metric}
    """.replace('{metric}', table_name))
    assert r.text == """
{"rows":[{"timestamp":{ts},"dc":"lga","host":"web01","value":95.27}]}
    """.strip().replace('{ts}', str(ts))


def test_put_single_point_with_second_timestamp():
    ts = now()
    ts_in_seconds = ts // 1000;
    table_name = table_prefix + str(ts)
    drop_table(table_name)

    execute_put_then_assert_success("""
{
    "metric": "{metric}",
    "timestamp": {ts},
    "value": 95.27,
    "tags": {
        "host": "web01",
        "dc": "lga"
    }
}
    """.replace('{metric}', table_name).replace('{ts}', str(ts_in_seconds)))

    r = execute_sql_query("""
SELECT timestamp, dc, host, value FROM {metric}
    """.replace('{metric}', table_name))
    assert r.text == """
{"rows":[{"timestamp":{ts},"dc":"lga","host":"web01","value":95.27}]}
    """.strip().replace('{ts}', str(ts))


def test_put_multi_points_with_different_tags_in_one_table():
    ts = now()
    table_name = table_prefix + str(ts)
    drop_table(table_name)

    execute_put_then_assert_success("""
[
    {
        "metric": "{metric}",
        "timestamp": {ts},
        "value": 18,
        "tags": {
           "host": "web01"
        }
    },
    {
        "metric": "{metric}",
        "timestamp": {ts},
        "value": 9,
        "tags": {
           "dc": "lga"
        }
    }
]
    """.replace('{metric}', table_name).replace('{ts}', str(ts)))

    r = execute_sql_query("""
SELECT timestamp, dc, host, value FROM {metric} ORDER BY value desc
    """.replace('{metric}', table_name))
    assert r.text == """
{"rows":[{"timestamp":{ts},"dc":null,"host":"web01","value":18.0},{"timestamp":{ts},"dc":"lga","host":null,"value":9.0}]}
    """.strip().replace('{ts}', str(ts))


# CeresDB internal error: "Column: value in table: ??? data type is not same, expected: bigint, actual: double"
def test_put_multi_points_with_different_datatype_in_one_table():
    ts = now()
    table_name = table_prefix + str(ts)
    drop_table(table_name)

    execute_put_then_assert_success("""
[
    {
        "metric": "{metric}",
        "timestamp": {ts},
        "value": 18,
        "tags": {
           "host": "web01",
           "dc": "lga"
        }
    },
    {
        "metric": "{metric}",
        "timestamp": {ts},
        "value": 9.999,
        "tags": {
            "host": "web02",
           "dc": "lga"
        }
    }
]
    """.replace('{metric}', table_name).replace('{ts}', str(ts)))


def test_put_multi_points_in_multi_table():
    ts = now()
    table_name = table_prefix + str(ts)
    table2_name = table2_prefix + str(ts)
    drop_table(table_name)
    drop_table(table2_name)

    execute_put_then_assert_success("""
[
    {
        "metric": "{metric}",
        "timestamp": {ts},
        "value": 18,
        "tags": {
           "host": "web01",
           "dc": "lga"
        }
    },
    {
        "metric": "{metric2}",
        "timestamp": {ts},
        "value": 9,
        "tags": {
            "host": "web02",
           "dc": "lga"
        }
    }
]
    """.replace('{metric}', table_name).replace('{metric2}', table2_name).replace('{ts}', str(ts)))

    r = execute_sql_query("""
SELECT timestamp, dc, host, value FROM {metric}
    """.replace('{metric}', table_name))
    assert r.text == """
{"rows":[{"timestamp":{ts},"dc":"lga","host":"web01","value":18.0}]}
    """.strip().replace('{ts}', str(ts))

    r = execute_sql_query("""
SELECT timestamp, dc, host, value FROM {metric}
    """.replace('{metric}', table2_name))
    assert r.text == """
{"rows":[{"timestamp":{ts},"dc":"lga","host":"web02","value":9.0}]}
    """.strip().replace('{ts}', str(ts))


def main():
    print("OpenTSDB test start.")

    test_put_validate_error()

    test_put_single_point_with_int_value()
    test_put_single_point_with_float_value()
    test_put_single_point_with_second_timestamp()

    test_put_multi_points_with_different_tags_in_one_table()
    test_put_multi_points_with_different_datatype_in_one_table()
    test_put_multi_points_in_multi_table()

    print("OpenTSDB test finished.")


if __name__ == '__main__':
    main()
