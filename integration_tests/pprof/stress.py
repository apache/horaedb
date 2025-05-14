#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# coding: utf-8

import random
import string
import sys
import time

import requests

api_root = 'http://localhost:5440'
headers = {
    'Content-Type': 'application/json'
}


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


def generate_random_string(length):
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(length))


def create_test_tables(count=3):
    table_names = []

    for t in range(count):
        ts = now()
        table_name = f"pprof_test_{ts}_{t}"
        table_names.append(table_name)

        sql = f"""CREATE TABLE {table_name} (
            item_id uint64 NOT NULL, 
            t timestamp NOT NULL, 
            name string TAG, 
            description string, 
            value1 double NOT NULL, 
            value2 double, 
            value3 double, 
            value4 double, 
            value5 double, 
            metric string TAG, 
            source string TAG, 
            status string TAG, 
            PRIMARY KEY(item_id,t), 
            TIMESTAMP KEY(t)
        ) ENGINE=Analytic with (enable_ttl="false", write_buffer_size="33554432")"""

        execute_sql(sql)
        show_create_table(table_name)
    return table_names


def insert_test_data(table_names, batch_size=100, batch_count=15):
    total_rows = 0

    for table_name in table_names:
        for batch in range(batch_count):
            values = []
            for i in range(batch_size):
                values.append(f"({i + batch * batch_size}, {now()}, 'test_{i}_{batch}', "
                              f"'{generate_random_string(300)}', "
                              f"{random.random() * 1000}, {random.random() * 1000}, "
                              f"{random.random() * 1000}, {random.random() * 1000}, "
                              f"{random.random() * 1000}, "
                              f"'{generate_random_string(20)}', "
                              f"'{generate_random_string(30)}', "
                              f"'{random.choice(['active', 'inactive', 'warning', 'critical', 'unknown'])}')")

            sql = f"""INSERT INTO {table_name} (
                item_id, t, name, description, 
                value1, value2, value3, value4, value5, 
                metric, source, status
            ) VALUES {", ".join(values)}"""
            execute_sql(sql)
            total_rows += batch_size

            # Execute some simple queries after each batch
            for _ in range(5):
                query_sql = f"""SELECT * FROM {table_name} 
                    WHERE t > {now() - 3600000} 
                    ORDER BY t DESC 
                    LIMIT {random.randint(10, 50)}"""
                execute_sql(query_sql)


def execute_complex_queries(table_names, query_count=10):
    query_executed = 0

    for i in range(query_count):
        table_name = random.choice(table_names)

        # Aggression
        query = f"""SELECT metric, source, status, 
            AVG(value1) as avg_value1, 
            MAX(value2) as max_value2, 
            MIN(value3) as min_value3, 
            SUM(value4) as sum_value4, 
            COUNT(*) as count 
        FROM {table_name} 
        GROUP BY metric, source, status 
        ORDER BY count DESC LIMIT 100"""
        execute_sql(query)
        query_executed += 1

        # Join
        if len(table_names) > 1:
            table1, table2 = table_names[0], table_names[1]
            join_query = f"""SELECT a.item_id, a.t, a.name, a.value1, b.value1 as value1_b 
                FROM {table1} as a 
                JOIN {table2} as b ON a.item_id = b.item_id 
                LIMIT 100"""
            execute_sql(join_query)
            query_executed += 1


def cleanup_test_tables(table_names):
    for table_name in table_names:
        try:
            drop_table(table_name)
        except Exception as e:
            print(f"Failed to drop table {table_name}: {e}")


def main():
    print("Starting stress test...")
    start_time = time.time()
    table_names = []
    try:
        table_names = create_test_tables()
        insert_test_data(table_names)
        execute_complex_queries(table_names)
    except Exception as e:
        print(f"Error during SQL stress test: {e}")
        sys.exit(1)
    finally:
        cleanup_test_tables(table_names)
        end_time = time.time()
        print(f"Stress test took {end_time - start_time:.2f} seconds")


if __name__ == "__main__":
    main()
