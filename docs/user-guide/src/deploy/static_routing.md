# Static Routing
This guide shows how to deloy a CeresDB cluster with static, rule-based routing.

The crucial point here is that CeresDB server provides routing function according to the config so what we need to do is just a valid config which contains the routing rules and then ship the config to every CeresDB instance in the cluster.

## Target
First of all, let's assume that our target is to deploy a cluster consisting of two CeresDB instances on the same machine. And a large cluster of more CeresDB instances can deployed according to the two-instance example.

## Prepare Config
Suppose the basic config of CeresDB is:
```toml
bind_addr = "0.0.0.0"
http_port = 5440
grpc_port = 8831
log_level = "info"
enable_cluster = true

[analytic]
wal_path = "/tmp/ceresdb"

[analytic.storage]
type = "Cache"

[analytic.storage.local_store]
type = "Local"
data_path = "/tmp/ceresdb"
```

In order to deploy three CeresDB instances on the same machine, the config should choose different ports to serve and separate data directory to store data.

Say the CeresDB_0's config is:
```toml
bind_addr = "0.0.0.0"
http_port = 5440
grpc_port = 8831
log_level = "info"
enable_cluster = true

[analytic]
wal_path = "/tmp/ceresdb_0"

[analytic.storage]
type = "Cache"

[analytic.storage.local_store]
type = "Local"
data_path = "/tmp/ceresdb_0"
```

The CeresDB_1's config is:
```toml
bind_addr = "0.0.0.0"
http_port = 15440
grpc_port = 18831
log_level = "info"
enable_cluster = true

[analytic]
wal_path = "/tmp/ceresdb_1"

[analytic.storage]
type = "Cache"

[analytic.storage.local_store]
type = "Local"
data_path = "/tmp/ceresdb_1"
```

Then we should define the common part -- schema&shard declaration and routing rules.

Here is the config for schema&shard declaration:
```toml
[[meta_client.cluster_view.schema_shards]]
schema = 'public_0'
[[meta_client.cluster_view.schema_shards.shard_views]]
shard_id = 0
[meta_client.cluster_view.schema_shards.shard_views.node]
addr = '127.0.0.1'
port = 8831
[[meta_client.cluster_view.schema_shards.shard_views]]
shard_id = 1
[meta_client.cluster_view.schema_shards.shard_views.node]
addr = '127.0.0.1'
port = 8831

[[meta_client.cluster_view.schema_shards]]
schema = 'public_1'
[[meta_client.cluster_view.schema_shards.shard_views]]
shard_id = 0
[meta_client.cluster_view.schema_shards.shard_views.node]
addr = '127.0.0.1'
port = 8831
[[meta_client.cluster_view.schema_shards.shard_views]]
shard_id = 1
[meta_client.cluster_view.schema_shards.shard_views.node]
addr = '127.0.0.1'
port = 18831
```

In the config above, two schemas are declared:
* `public_0` has two shards served by `CeresDB_0`.
* `public_1` has two shards served by both `CeresDB_0` and `CeresDB_1`.

Provided with shcema&shard declaration, routing rules can be defined and here is an example of prefix rule:
```toml
[[route_rules.prefix_rules]]
schema = 'public_0'
prefix = 'auto'
shard = 0
```
This rule means that all the table with `auto` prefix belonging to `public_0` should be routed to `shard_0` of `public_0`, that is to say, `CeresDB_0`. As for the other tables whose names are not prefixed by `auto` will be routed by hash to `shard_0` and `shard_1` of `public_0`.

Besides prefix rule, we can also define a hash rule:
```toml
[[route_rules.hash_rules]]
schema = 'public_1'
shards = [1]
```
This rule tells CeresDB to route `public_1`'s tables to `shard_1` of `public_1`, that is to say, `CeresDB_1`. And it is worth mentioning that`shard_0` of `public_1` will receive no requests if this rule is applied.

For now, we can provide the full example config for `CeresDB_0` and `CeresDB_1`:
```toml
bind_addr = "0.0.0.0"
http_port = 5440
grpc_port = 8831
log_level = "info"
enable_cluster = true

[analytic]
wal_path = "/tmp/ceresdb_0"

[analytic.storage]
type = "Cache"

[analytic.storage.local_store]
type = "Local"
data_path = "/tmp/ceresdb_0"

[[meta_client.cluster_view.schema_shards]]
schema = 'public_0'
[[meta_client.cluster_view.schema_shards.shard_views]]
shard_id = 0
[meta_client.cluster_view.schema_shards.shard_views.node]
addr = '127.0.0.1'
port = 8831
[[meta_client.cluster_view.schema_shards.shard_views]]
shard_id = 1
[meta_client.cluster_view.schema_shards.shard_views.node]
addr = '127.0.0.1'
port = 8831

[[meta_client.cluster_view.schema_shards]]
schema = 'public_1'
[[meta_client.cluster_view.schema_shards.shard_views]]
shard_id = 0
[meta_client.cluster_view.schema_shards.shard_views.node]
addr = '127.0.0.1'
port = 8831
[[meta_client.cluster_view.schema_shards.shard_views]]
shard_id = 1
[meta_client.cluster_view.schema_shards.shard_views.node]
addr = '127.0.0.1'
port = 18831

[[route_rules.prefix_rules]]
schema = 'public_0'
prefix = 'auto'
shard = 0

[[route_rules.hash_rules]]
schema = 'public_1'
shards = [1]
```

```yaml
bind_addr = "0.0.0.0"
http_port = 15440
grpc_port = 18831
log_level = "info"
enable_cluster = true

[analytic]
wal_path = "/tmp/ceresdb_1"

[analytic.storage]
type = "Cache"

[analytic.storage.local_store]
type = "Local"
data_path = "/tmp/ceresdb_1"

[[meta_client.cluster_view.schema_shards]]
schema = 'public_0'
[[meta_client.cluster_view.schema_shards.shard_views]]
shard_id = 0
[meta_client.cluster_view.schema_shards.shard_views.node]
addr = '127.0.0.1'
port = 8831
[[meta_client.cluster_view.schema_shards.shard_views]]
shard_id = 1
[meta_client.cluster_view.schema_shards.shard_views.node]
addr = '127.0.0.1'
port = 8831

[[meta_client.cluster_view.schema_shards]]
schema = 'public_1'
[[meta_client.cluster_view.schema_shards.shard_views]]
shard_id = 0
[meta_client.cluster_view.schema_shards.shard_views.node]
addr = '127.0.0.1'
port = 8831
[[meta_client.cluster_view.schema_shards.shard_views]]
shard_id = 1
[meta_client.cluster_view.schema_shards.shard_views.node]
addr = '127.0.0.1'
port = 18831

[[route_rules.prefix_rules]]
schema = 'public_0'
prefix = 'auto'
shard = 0

[[route_rules.hash_rules]]
schema = 'public_1'
shards = [1]
```

Let's call the two different config files as `config_0.yaml` and `config_1.ymal` but you should know in the real environment the different CeresDB intances can be deloyed across different machines, that is to say, there is no need to choose different ports and data directories for different CeresDB instances so that all the CeresDB instances can share one exactly **same** config file.

## Start CeresDBs
After the configs are prepared, what we should to do is to start CeresDB container with the specific config.

Just run the commands below:
```shell
docker run -d -t --name ceresdb -p 5440:5440 -p 8831:8831 -v $(pwd)/config_0.yaml:/etc/ceresdbx/ceresdb.toml  ceresdb_0
docker run -d -t --name ceresdb -p 15440:15440 -p 18831:18831 -v $(pwd)/config_0.yaml:/etc/ceresdbx/ceresdb.toml ceresdb_1
```

After the two containers are created and starting running, read and write requests can be served by the CeresDB cluster consisting of two instances.

