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

const (
	HTTP = "http://"
	API  = "/api/v1"

	APIClusters         = API + "/clusters"
	APIClustersDiagnose = API + "/clusters/diagnose"

	RootMetaAddr = "meta_addr"
	RootCluster  = "cluster_name"
)

var clustersListHeader = []string{"ID", "Name", "ShardTotal", "TopologyType", "ProcedureExecutingBatchSize", "CreatedAt", "ModifiedAt"}
var clustersDiagnoseHeader = []string{"unregistered_shards", "unready_shards:shard_id", "unready_shards:node_name", "unready_shards:status"}
