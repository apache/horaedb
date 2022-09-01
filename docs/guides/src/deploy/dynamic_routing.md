# Dynamic Routing
This guide shows how to deploy a CeresDB cluster with CeresMeta.

## Target
First, let's assume that our target is to deploy a cluster consisting of two CeresDB instances on the same machine. And a large cluster of more CeresDB instances can deploy according to the two-instances example.

## Start CeresDBs
You can use the following command to create a CeresDB cluster with two instances.
1. Start CeresMeta first
   Refer to [CeresMeta](https://github.com/CeresDB/ceresmeta)

2. Prepare config of CeresDB
* Replace `{meta_addr}` with the actual address of CeresMeta in config file
 
```toml
# {project_path}/docs/example-cluster-0.toml
bind_addr = "0.0.0.0"
http_port = 5440
grpc_port = 8831
mysql_port = 3307
log_level = "info"
deploy_mode = "Cluster"

[analytic]
wal_path = "/tmp/ceresdb"
sst_data_cache_cap = 10000
sst_meta_cache_cap = 10000

[analytic.storage]
type = "Local"
data_path = "/tmp/ceresdb"

[cluster]
cmd_channel_buffer_size = 10

[cluster.node]
addr = "127.0.0.1"
port = 8831

[cluster.meta_client]
# Only support "defaultCluster" currently.
cluster_name = "defaultCluster"
meta_addr = "{meta_addr}:2379"
lease = "10s"
timeout = "5s"
```

```toml
# {project_path}/docs/example-cluster-1.toml
bind_addr = "0.0.0.0"
http_port = 5441
grpc_port = 8832
mysql_port = 13307
log_level = "info"
deploy_mode = "Cluster"

[analytic]
wal_path = "/tmp/ceresdb2"
sst_data_cache_cap = 10000
sst_meta_cache_cap = 10000

[analytic.storage]
type = "Local"
data_path = "/tmp/ceresdb2"

[cluster]
cmd_channel_buffer_size = 10

[cluster.node]
addr = "127.0.0.1"
port = 8832

[cluster.meta_client]
# Only support "defaultCluster" currently.
cluster_name = "defaultCluster"
meta_addr = "{meta_addr}:2379"
lease = "10s"
timeout = "5s"
```

3. Start CeresDB instances
* You need to replace `{project_path}` with the actual project path

```bash
# Update address of CeresMeta in CeresDB config.
docker run -d --name ceresdb-server \
  -p 8831:8831 \
  -p 3307:3307 \
  -p 5440:5440 \
  -v {project_path}/docs/example-cluster-0.toml:/etc/ceresdb/ceresdb.toml \
  ceresdb/ceresdb-server:v0.3.1
  
docker run -d --name ceresdb-server2 \
  -p 8832:8832 \
  -p 13307:13307 \
  -p 5441:5441 \
  -v {project_path}/docs/example-cluster-1.toml:/etc/ceresdb/ceresdb.toml \
  ceresdb/ceresdb-server:v0.3.1
```