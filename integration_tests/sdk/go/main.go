package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/CeresDB/ceresdb-client-go/ceresdb"
)

var endpoint = "127.0.0.1:8831"

const table = "godemo"

func init() {
	if v := os.Getenv("CERESDB_ADDR"); v != "" {
		endpoint = v
	}
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

func query(ctx context.Context, client ceresdb.Client, ts int64, addNewColumn bool) error {
	resp, err := client.SQLQuery(ctx, ceresdb.SQLQueryRequest{
		Tables: []string{table},
		SQL:    fmt.Sprintf("select * from %s where timestamp = %d order by name", table, ts),
	})
	if err != nil {
		return err
	}

	if len(resp.Rows) != 2 {
		return fmt.Errorf("expect 2 rows, current: %+v", len(resp.Rows))
	}

	row0 := []ceresdb.Value{
		ceresdb.NewUint64Value(4024844655630594205),
		ceresdb.NewInt64Value(ts),
		ceresdb.NewStringValue("tag-0"),
		ceresdb.NewInt64Value(0)}

	row1 := []ceresdb.Value{
		ceresdb.NewUint64Value(14230010170561829440),
		ceresdb.NewInt64Value(ts),
		ceresdb.NewStringValue("tag-1"),
		ceresdb.NewInt64Value(1),
	}

	if addNewColumn {
		row0[0] = ceresdb.NewUint64Value(8341999341185504339)
		row1[0] = ceresdb.NewUint64Value(4452331151453582498)
		row0 = append(row0, ceresdb.NewInt64Value(0), ceresdb.NewStringValue("new-tag-0"))
		row1 = append(row1, ceresdb.NewInt64Value(1), ceresdb.NewStringValue("new-tag-1"))
	}

	if err := ensureRow(row0,
		resp.Rows[0].Columns()); err != nil {
		return err
	}

	return ensureRow(row1, resp.Rows[1].Columns())
}

func ddl(ctx context.Context, client ceresdb.Client, sql string) (uint32, error) {
	resp, err := client.SQLQuery(ctx, ceresdb.SQLQueryRequest{
		Tables: []string{table},
		SQL:    sql,
	})
	if err != nil {
		return 0, err
	}

	return resp.AffectedRows, nil
}

func checkAutoCreateTable(ctx context.Context, client ceresdb.Client) error {
	if _, err := ddl(ctx, client, "drop table if exists "+table); err != nil {
		return err
	}

	ts := currentMS()
	if err := write(ctx, client, ts, false); err != nil {
		return err
	}

	if err := query(ctx, client, ts, false); err != nil {
		return err
	}

	return nil
}

func checkAutoAddColumns(ctx context.Context, client ceresdb.Client) error {
	ts := currentMS()
	if err := write(ctx, client, ts, true); err != nil {
		return err
	}

	if err := query(ctx, client, ts, true); err != nil {
		return err
	}

	return nil
}

func dropTable(ctx context.Context, client ceresdb.Client) error {
	affected, err := ddl(ctx, client, "drop table "+table)
	if err != nil {
		return err
	}

	if affected != 0 {
		panic(fmt.Sprintf("drop table expected 0, actual is %d", affected))
	}
	return nil
}

func main() {
	fmt.Printf("Begin test, endpoint %s...\n", endpoint)

	client, err := ceresdb.NewClient(endpoint, ceresdb.Direct,
		ceresdb.WithDefaultDatabase("public"),
	)
	if err != nil {
		panic(err)
	}

	ctx := context.TODO()
	if err = checkAutoCreateTable(ctx, client); err != nil {
		panic(err)
	}

	if err = checkAutoAddColumns(ctx, client); err != nil {
		panic(err)
	}

	if err = dropTable(ctx, client); err != nil {
		panic(err)
	}

	fmt.Println("Test done")
}

func currentMS() int64 {
	return time.Now().UnixMilli()
}
