package main

import (
	"context"
	"fmt"

	"github.com/CeresDB/ceresdb-client-go/ceresdb"
)

const table = "godemo"
const partitionTable = "godemoPartition"

func createTable(ctx context.Context, client ceresdb.Client, timestampName string) error {
	_, err := ddl(ctx, client, table, fmt.Sprintf("create table %s (`%s` timestamp not null, name string tag, value int64,TIMESTAMP KEY(%s))", table, timestampName, timestampName))
	return err
}

func write(ctx context.Context, client ceresdb.Client, ts int64, addNewColumn bool) error {
	points := make([]ceresdb.Point, 0, 2)
	for i := 0; i < 2; i++ {
		builder := ceresdb.NewPointBuilder(table).
			SetTimestamp(ts).
			AddTag("name", ceresdb.NewStringValue(fmt.Sprintf("tag-%d", i))).
			AddField("value", ceresdb.NewInt64Value(int64(i)))

		if addNewColumn {
			builder = builder.AddTag("new_tag", ceresdb.NewStringValue(fmt.Sprintf("new-tag-%d", i))).
				AddField("new_field", ceresdb.NewInt64Value(int64(i)))
		}

		point, err := builder.Build()

		if err != nil {
			return err
		}
		points = append(points, point)
	}

	resp, err := client.Write(ctx, ceresdb.WriteRequest{
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

func ensureRow(expectedVals []ceresdb.Value, actualRow []ceresdb.Column) error {
	for i, expected := range expectedVals {
		if actual := actualRow[i].Value(); actual != expected {
			return fmt.Errorf("expected: %+v, actual: %+v", expected, actual)
		}
	}
	return nil

}

func query(ctx context.Context, client ceresdb.Client, ts int64, timestampName string, addNewColumn bool) error {
	sql := fmt.Sprintf("select timestamp, name, value from %s where %s = %d order by name", table, timestampName, ts)
	if addNewColumn {
		sql = fmt.Sprintf("select timestamp, name, value, new_tag, new_field from %s where %s = %d order by name", table, timestampName, ts)
	}
	resp, err := client.SQLQuery(ctx, ceresdb.SQLQueryRequest{
		Tables: []string{table},
		SQL:    sql,
	})
	if err != nil {
		return err
	}

	if len(resp.Rows) != 2 {
		return fmt.Errorf("expect 2 rows, current: %+v", len(resp.Rows))
	}

	row0 := []ceresdb.Value{
		ceresdb.NewInt64Value(ts),
		ceresdb.NewStringValue("tag-0"),
		ceresdb.NewInt64Value(0)}

	row1 := []ceresdb.Value{
		ceresdb.NewInt64Value(ts),
		ceresdb.NewStringValue("tag-1"),
		ceresdb.NewInt64Value(1),
	}

	if addNewColumn {
		row0 = append(row0, ceresdb.NewStringValue("new-tag-0"), ceresdb.NewInt64Value(0))
		row1 = append(row1, ceresdb.NewStringValue("new-tag-1"), ceresdb.NewInt64Value(1))
	}

	if err := ensureRow(row0,
		resp.Rows[0].Columns()); err != nil {
		return err
	}

	return ensureRow(row1, resp.Rows[1].Columns())
}

func ddl(ctx context.Context, client ceresdb.Client, tableName string, sql string) (uint32, error) {
	resp, err := client.SQLQuery(ctx, ceresdb.SQLQueryRequest{
		Tables: []string{tableName},
		SQL:    sql,
	})
	if err != nil {
		return 0, err
	}

	return resp.AffectedRows, nil
}

func writeAndQuery(ctx context.Context, client ceresdb.Client, timestampName string) error {
	ts := currentMS()
	if err := write(ctx, client, ts, false); err != nil {
		return err
	}

	if err := query(ctx, client, ts, timestampName, false); err != nil {
		return err
	}

	return nil
}

func writeAndQueryWithNewColumns(ctx context.Context, client ceresdb.Client, timestampName string) error {
	ts := currentMS()
	if err := write(ctx, client, ts, true); err != nil {
		return err
	}

	if err := query(ctx, client, ts, timestampName, true); err != nil {
		return err
	}

	return nil
}

func dropTable(ctx context.Context, client ceresdb.Client, table string) error {
	affected, err := ddl(ctx, client, table, "drop table if exists "+table)
	if err != nil {
		return err
	}

	if affected != 0 {
		panic(fmt.Sprintf("drop table expected 0, actual is %d", affected))
	}
	return nil
}
