package main

import (
	"context"
	"fmt"

	"github.com/CeresDB/ceresdb-client-go/ceresdb"
)

func checkAddColumn(ctx context.Context, client ceresdb.Client) error {
	err := dropTable(ctx, client, partitionTable)
	if err != nil {
		return err
	}

	_, err = ddl(ctx, client, partitionTable, "CREATE TABLE `godemoPartition`(\n    `name`string TAG,\n    `id` int TAG,\n    `value` int64 NOT NULL,\n    `t` timestamp NOT NULL,\n    TIMESTAMP KEY(t)\n    ) PARTITION BY KEY(name) PARTITIONS 4 ENGINE = Analytic")
	if err != nil {
		return err
	}

	_, err = ddl(ctx, client, partitionTable, "ALTER TABLE `godemoPartition` ADD COLUMN (b string);")
	if err != nil {
		return err
	}

	ts := currentMS()
	writePartitionTable(ctx, client, ts)

	if err := writePartitionTable(ctx, client, ts); err != nil {
		return err
	}

	_, err = ddl(ctx, client, partitionTable, "ALTER TABLE `godemoPartition` ADD COLUMN (bb string TAG);")
	if err != nil {
		return err
	}

	writePartitionTable2(ctx, client, ts)

	if err := writePartitionTable2(ctx, client, ts); err != nil {
		return err
	}

	if err := queryPartitionTable(ctx, client, ts, "t"); err != nil {
		return err
	}

	return nil
}

func writePartitionTable(ctx context.Context, client ceresdb.Client, ts int64) error {
	points := make([]ceresdb.Point, 0, 2)
	for i := 0; i < 2; i++ {
		builder := ceresdb.NewPointBuilder(partitionTable).
			SetTimestamp(ts).
			AddTag("name", ceresdb.NewStringValue(fmt.Sprintf("tag-%d", i))).
			AddField("value", ceresdb.NewInt64Value(int64(i))).
			//AddTag("bb", ceresdb.NewStringValue("sstag")).
			AddField("b", ceresdb.NewStringValue("ss"))

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

func writePartitionTable2(ctx context.Context, client ceresdb.Client, ts int64) error {
	points := make([]ceresdb.Point, 0, 2)
	for i := 0; i < 2; i++ {
		builder := ceresdb.NewPointBuilder(partitionTable).
			SetTimestamp(ts).
			AddTag("name", ceresdb.NewStringValue(fmt.Sprintf("tag-%d", i))).
			AddField("value", ceresdb.NewInt64Value(int64(i))).
			AddTag("bb", ceresdb.NewStringValue("sstag")).
			AddField("b", ceresdb.NewStringValue("ss"))

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

func queryPartitionTable(ctx context.Context, client ceresdb.Client, ts int64, timestampName string) error {
	sql := fmt.Sprintf("select t, name, value,b,bb from %s where %s = %d order by name,bb", partitionTable, timestampName, ts)

	resp, err := client.SQLQuery(ctx, ceresdb.SQLQueryRequest{
		Tables: []string{partitionTable},
		SQL:    sql,
	})
	if err != nil {
		return err
	}

	if len(resp.Rows) != 4 {
		return fmt.Errorf("expect 2 rows, current: %+v", len(resp.Rows))
	}

	row0 := []ceresdb.Value{
		ceresdb.NewInt64Value(ts),
		ceresdb.NewStringValue("tag-0"),
		ceresdb.NewInt64Value(0),
		ceresdb.NewStringValue("ss"),
		ceresdb.NewStringValue("sstag"),
	}

	row1 := []ceresdb.Value{
		ceresdb.NewInt64Value(ts),
		ceresdb.NewStringValue("tag-0"),
		ceresdb.NewInt64Value(0),
		ceresdb.NewStringValue("ss"),
	}

	row2 := []ceresdb.Value{
		ceresdb.NewInt64Value(ts),
		ceresdb.NewStringValue("tag-1"),
		ceresdb.NewInt64Value(1),
		ceresdb.NewStringValue("ss"),
		ceresdb.NewStringValue("sstag"),
	}

	row3 := []ceresdb.Value{
		ceresdb.NewInt64Value(ts),
		ceresdb.NewStringValue("tag-1"),
		ceresdb.NewInt64Value(1),
		ceresdb.NewStringValue("ss"),
	}

	if err := ensureRow(row0,
		resp.Rows[0].Columns()); err != nil {
		return err
	}
	if err := ensureRow(row1,
		resp.Rows[1].Columns()); err != nil {
		return err
	}
	if err := ensureRow(row2,
		resp.Rows[2].Columns()); err != nil {
		return err
	}

	return ensureRow(row3, resp.Rows[3].Columns())
}
