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

func write(ctx context.Context, client ceresdb.Client, ts int64) error {
	points := make([]ceresdb.Point, 0, 2)
	for i := 0; i < 2; i++ {
		point, err := ceresdb.NewPointBuilder(table).
			SetTimestamp(ts).
			AddTag("name", ceresdb.NewStringValue(fmt.Sprintf("tag-%d", i))).
			AddField("value", ceresdb.NewInt64Value(int64(i))).
			Build()
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

func query(ctx context.Context, client ceresdb.Client, ts int64) error {
	resp, err := client.SQLQuery(ctx, ceresdb.SQLQueryRequest{
		Tables: []string{table},
		SQL:    fmt.Sprintf("select * from %s where timestamp = %d", table, ts),
	})
	if err != nil {
		return err
	}

	if len(resp.Rows) != 2 {
		return fmt.Errorf("expect 2 rows, current: %+v", len(resp.Rows))
	}

	if err := ensureRow([]ceresdb.Value{
		ceresdb.NewUint64Value(4024844655630594205),
		ceresdb.NewInt64Value(ts),
		ceresdb.NewStringValue("tag-0"),
		ceresdb.NewInt64Value(0),
	}, resp.Rows[0].Columns()); err != nil {
		return err
	}

	return ensureRow([]ceresdb.Value{
		ceresdb.NewUint64Value(14230010170561829440),
		ceresdb.NewInt64Value(ts),
		ceresdb.NewStringValue("tag-1"),
		ceresdb.NewInt64Value(1),
	}, resp.Rows[1].Columns())
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

func main() {
	fmt.Printf("Begin test, endpoint %s...\n", endpoint)

	client, err := ceresdb.NewClient(endpoint, ceresdb.Direct,
		ceresdb.WithDefaultDatabase("public"),
	)
	if err != nil {
		panic(err)
	}

	ctx := context.TODO()
	if _, err := ddl(ctx, client, "drop table if exists "+table); err != nil {
		panic(err)
	}

	ts := currentMS()
	if err := write(ctx, client, ts); err != nil {
		panic(err)
	}

	if err := query(ctx, client, ts); err != nil {
		panic(err)
	}

	affected, err := ddl(ctx, client, "drop table "+table)
	if err != nil {
		panic(err)
	}

	if affected != 0 {
		panic(fmt.Sprintf("drop table expected 0, actual is %d", affected))
	}

	fmt.Println("Test done")
}

func currentMS() int64 {
	return time.Now().UnixMilli()
}
