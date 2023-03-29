package main

import (
	"context"

	"github.com/CeresDB/ceresdb-client-go/ceresdb"
)

func checkAutoAddColumnsWithCreateTable(ctx context.Context, client ceresdb.Client) error {
	timestampName := "t"

	err := dropTable(ctx, client)
	if err != nil {
		return err
	}

	err = createTable(ctx, client, timestampName)
	if err != nil {
		return err
	}

	err = writeAndQuery(ctx, client, timestampName)
	if err != nil {
		return err
	}

	return writeAndQueryWithNewColumns(ctx, client, timestampName)
}
