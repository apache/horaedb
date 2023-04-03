package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/CeresDB/ceresdb-client-go/ceresdb"
)

var endpoint = "127.0.0.1:8831"

func init() {
	if v := os.Getenv("CERESDB_ADDR"); v != "" {
		endpoint = v
	}
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
	if err = checkAutoAddColumns(ctx, client); err != nil {
		panic(err)
	}

	if err = checkAutoAddColumnsWithCreateTable(ctx, client); err != nil {
		panic(err)
	}

	fmt.Println("Test done")
}

func currentMS() int64 {
	return time.Now().UnixMilli()
}
