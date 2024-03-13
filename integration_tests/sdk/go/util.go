/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package main

import (
	"context"
	"fmt"

	"github.com/apache/incubator-horaedb-client-go/horaedb"
)

const table = "godemo"
const partitionTable = "godemoPartition"

func createTable(ctx context.Context, client horaedb.Client, timestampName string) error {
	_, err := ddl(ctx, client, table, fmt.Sprintf("create table %s (`%s` timestamp not null, name string tag, value int64,TIMESTAMP KEY(%s))", table, timestampName, timestampName))
	return err
}

func write(ctx context.Context, client horaedb.Client, ts int64, addNewColumn bool) error {
	points := make([]horaedb.Point, 0, 2)
	for i := 0; i < 2; i++ {
		builder := horaedb.NewPointBuilder(table).
			SetTimestamp(ts).
			AddTag("name", horaedb.NewStringValue(fmt.Sprintf("tag-%d", i))).
			AddField("value", horaedb.NewInt64Value(int64(i)))

		if addNewColumn {
			builder = builder.AddTag("new_tag", horaedb.NewStringValue(fmt.Sprintf("new-tag-%d", i))).
				AddField("new_field", horaedb.NewInt64Value(int64(i)))
		}

		point, err := builder.Build()

		if err != nil {
			return err
		}
		points = append(points, point)
	}

	resp, err := client.Write(ctx, horaedb.WriteRequest{
		Points: points,
	})
	if err != nil {
		return err
	}

	if resp.Success != 2 {
		return fmt.Errorf("write failed, resp: %+v", resp)
	}

	return nil
}

func ensureRow(expectedVals []horaedb.Value, actualRow []horaedb.Column) error {
	for i, expected := range expectedVals {
		if actual := actualRow[i].Value(); actual != expected {
			return fmt.Errorf("expected: %+v, actual: %+v", expected, actual)
		}
	}
	return nil

}

func query(ctx context.Context, client horaedb.Client, ts int64, timestampName string, addNewColumn bool) error {
	sql := fmt.Sprintf("select timestamp, name, value from %s where %s = %d order by name", table, timestampName, ts)
	if addNewColumn {
		sql = fmt.Sprintf("select timestamp, name, value, new_tag, new_field from %s where %s = %d order by name", table, timestampName, ts)
	}
	resp, err := client.SQLQuery(ctx, horaedb.SQLQueryRequest{
		Tables: []string{table},
		SQL:    sql,
	})
	if err != nil {
		return err
	}

	if len(resp.Rows) != 2 {
		return fmt.Errorf("expect 2 rows, current: %+v", len(resp.Rows))
	}

	row0 := []horaedb.Value{
		horaedb.NewInt64Value(ts),
		horaedb.NewStringValue("tag-0"),
		horaedb.NewInt64Value(0)}

	row1 := []horaedb.Value{
		horaedb.NewInt64Value(ts),
		horaedb.NewStringValue("tag-1"),
		horaedb.NewInt64Value(1),
	}

	if addNewColumn {
		row0 = append(row0, horaedb.NewStringValue("new-tag-0"), horaedb.NewInt64Value(0))
		row1 = append(row1, horaedb.NewStringValue("new-tag-1"), horaedb.NewInt64Value(1))
	}

	if err := ensureRow(row0,
		resp.Rows[0].Columns()); err != nil {
		return err
	}

	return ensureRow(row1, resp.Rows[1].Columns())
}

func ddl(ctx context.Context, client horaedb.Client, tableName string, sql string) (uint32, error) {
	resp, err := client.SQLQuery(ctx, horaedb.SQLQueryRequest{
		Tables: []string{tableName},
		SQL:    sql,
	})
	if err != nil {
		return 0, err
	}

	return resp.AffectedRows, nil
}

func writeAndQuery(ctx context.Context, client horaedb.Client, timestampName string) error {
	ts := currentMS()
	if err := write(ctx, client, ts, false); err != nil {
		return err
	}

	if err := query(ctx, client, ts, timestampName, false); err != nil {
		return err
	}

	return nil
}

func writeAndQueryWithNewColumns(ctx context.Context, client horaedb.Client, timestampName string) error {
	ts := currentMS()
	if err := write(ctx, client, ts, true); err != nil {
		return err
	}

	if err := query(ctx, client, ts, timestampName, true); err != nil {
		return err
	}

	return nil
}

func dropTable(ctx context.Context, client horaedb.Client, table string) error {
	affected, err := ddl(ctx, client, table, "drop table if exists "+table)
	if err != nil {
		return err
	}

	if affected != 0 {
		panic(fmt.Sprintf("drop table expected 0, actual is %d", affected))
	}
	return nil
}
