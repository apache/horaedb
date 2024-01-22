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

package operation

import (
	"fmt"
	"net/http"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/spf13/viper"
)

type Cluster struct {
	ID                          uint32 `json:"id"`
	Name                        string `json:"name"`
	MinNodeCount                uint32 `json:"minNodeCount"`
	ShardTotal                  uint32 `json:"shardTotal"`
	TopologyType                string `json:"topologyType"`
	ProcedureExecutingBatchSize uint32 `json:"procedureExecutingBatchSize"`
	CreatedAt                   uint64 `json:"createdAt"`
	ModifiedAt                  uint64 `json:"modifiedAt"`
}

type ClusterResponse struct {
	Status string    `json:"status"`
	Data   []Cluster `json:"data"`
}

type DiagnoseShardStatus struct {
	NodeName string `json:"node_name"`
	Status   string `json:"status"`
}

type DiagnoseShardResponse struct {
	// shardID -> nodeName
	UnregisteredShards []uint32                       `json:"unregistered_shards"`
	UnreadyShards      map[uint32]DiagnoseShardStatus `json:"unready_shards"`
}

func clusterUrl() string {
	return HTTP + viper.GetString(RootMetaAddr) + APIClusters
}
func diagnoseUrl() string {
	return HTTP + viper.GetString(RootMetaAddr) + APIClustersDiagnose + viper.GetString(RootCluster) + "/shards"
}

func ClustersList() {
	url := clusterUrl()
	var response ClusterResponse
	err := HttpUtil(http.MethodGet, url, nil, &response)
	if err != nil {
		fmt.Println(err)
	}

	t := tableWriter(clustersListHeader)
	for _, data := range response.Data {
		row := table.Row{data.ID, data.Name, data.ShardTotal, data.TopologyType, data.ProcedureExecutingBatchSize, FormatTimeMilli(int64(data.CreatedAt)), FormatTimeMilli(int64(data.ModifiedAt))}
		t.AppendRow(row)
	}
	fmt.Println(t.Render())
	t.Style()
}

func ClusterDiagnose() {
	url := clusterUrl()
	var response DiagnoseShardResponse
	err := HttpUtil(http.MethodGet, url, nil, &response)
	if err != nil {
		fmt.Println(err)
	}

	t := tableWriter(clustersDiagnoseHeader)
	row := table.Row{response.UnregisteredShards}
	t.AppendRow(row)
	for shardID, data := range response.UnreadyShards {
		row := table.Row{"", shardID, data.NodeName, data.Status}
		t.AppendRow(row)
	}
	fmt.Println(t.Render())
	t.Style()
}
