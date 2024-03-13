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
	"os"
	"time"

	"github.com/apache/incubator-horaedb-client-go/horaedb"
)

var endpoint = "127.0.0.1:8831"

func init() {
	if v := os.Getenv("HORAEDB_ADDR"); v != "" {
		endpoint = v
	}
}

func main() {
	fmt.Printf("Begin test, endpoint %s...\n", endpoint)

	client, err := horaedb.NewClient(endpoint, horaedb.Direct,
		horaedb.WithDefaultDatabase("public"),
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

	if err = checkPartitionTableAddColumn(ctx, client); err != nil {
		panic(err)
	}

	fmt.Println("Test done")
}

func currentMS() int64 {
	return time.Now().UnixMilli()
}
