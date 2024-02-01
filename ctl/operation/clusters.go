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
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/apache/incubator-horaedb/ctl/util"

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
	NodeName string `json:"nodeName"`
	Status   string `json:"status"`
}

type EnableScheduleRequest struct {
	Enable bool `json:"enable"`
}

type EnableScheduleResponse struct {
	Status string `json:"status"`
	Data   bool   `json:"data"`
}

type DiagnoseShard struct {
	// shardID -> nodeName
	UnregisteredShards []uint32                       `json:"unregisteredShards"`
	UnreadyShards      map[uint32]DiagnoseShardStatus `json:"unreadyShards"`
}

type DiagnoseShardResponse struct {
	Status string        `json:"status"`
	Data   DiagnoseShard `json:"data"`
}

func clusterUrl() string {
	return util.HTTP + viper.GetString(util.RootMetaAddr) + util.API + util.CLUSTERS
}

func diagnoseUrl() string {
	return util.HTTP + viper.GetString(util.RootMetaAddr) + util.DEBUG + "/diagnose" + "/" + viper.GetString(util.RootCluster) + "/shards"
}

func enableScheduleUrl() string {
	return util.HTTP + viper.GetString(util.RootMetaAddr) + util.DEBUG + util.CLUSTERS + "/" + viper.GetString(util.RootCluster) + "/enableSchedule"
}

func ClustersList() {
	url := clusterUrl()
	var response ClusterResponse
	err := util.HttpUtil(http.MethodGet, url, nil, &response)
	if err != nil {
		fmt.Println(err)
	}

	t := util.TableWriter(util.ClustersListHeader)
	for _, data := range response.Data {
		row := table.Row{data.ID, data.Name, data.ShardTotal, data.TopologyType, data.ProcedureExecutingBatchSize, util.FormatTimeMilli(int64(data.CreatedAt)), util.FormatTimeMilli(int64(data.ModifiedAt))}
		t.AppendRow(row)
	}
	fmt.Println(t.Render())
	t.Style()
}

func ClusterDiagnose() {
	url := diagnoseUrl()
	var response DiagnoseShardResponse
	err := util.HttpUtil(http.MethodGet, url, nil, &response)
	if err != nil {
		fmt.Println(err)
	}

	t := util.TableWriter(util.ClustersDiagnoseHeader)
	row := table.Row{response.Data.UnregisteredShards}
	t.AppendRow(row)
	for shardID, data := range response.Data.UnreadyShards {
		row := table.Row{"", shardID, data.NodeName, data.Status}
		t.AppendRow(row)
	}
	fmt.Println(t.Render())
	t.Style()
}

func ClusterScheduleGet() {
	url := enableScheduleUrl()
	var response EnableScheduleResponse
	err := util.HttpUtil(http.MethodGet, url, nil, &response)
	if err != nil {
		fmt.Println(err)
	}

	t := util.TableWriter(util.ClustersEnableScheduleHeader)
	row := table.Row{response.Data}
	t.AppendRow(row)
	fmt.Println(t.Render())
	t.Style()
}

func ClusterScheduleSet(enable bool) {
	url := enableScheduleUrl()
	request := EnableScheduleRequest{
		Enable: enable,
	}
	body, err := json.Marshal(request)
	if err != nil {
		fmt.Println(err)
	}

	var response EnableScheduleResponse
	err = util.HttpUtil(http.MethodPut, url, bytes.NewBuffer(body), &response)
	if err != nil {
		fmt.Println(err)
	}

	t := util.TableWriter(util.ClustersEnableScheduleHeader)
	row := table.Row{response.Data}
	t.AppendRow(row)
	fmt.Println(t.Render())
	t.Style()
}
