package main

import (
	"context"

	"github.com/apache/incubator-horaedb-client-go/horaedb"
)

func checkAutoAddColumns(ctx context.Context, client horaedb.Client) error {
	timestampName := "timestamp"
	err := dropTable(ctx, client, table)
	if err != nil {
		return err
	}

	err = writeAndQuery(ctx, client, timestampName)
	if err != nil {
		return err
	}

	return writeAndQueryWithNewColumns(ctx, client, timestampName)
}
