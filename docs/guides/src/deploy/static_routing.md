# Static Routing
This guide shows how to deploy a CeresDB cluster with static, rule-based routing.

The crucial point here is that CeresDB server provides configurable routing function on table name so what we need is just a valid config containing routing rules which will be shipped to every CeresDB instance in the cluster.

## Target
First, let's assume that our target is to deploy a cluster consisting of two CeresDB instances on the same machine. And a large cluster of more CeresDB instances can deploy according to the two-instances example.

## Prepare Config
### Basic
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
type = "Local"
data_path = "/tmp/ceresdb"
```

In order to deploy two CeresDB instances on the same machine, the config should choose different ports to serve and data directories to store data.

Say the `CeresDB_0`'s config is:
```toml
bind_addr = "0.0.0.0"
http_port = 5440
grpc_port = 8831
log_level = "info"
enable_cluster = true

[analytic]
wal_path = "/tmp/ceresdb_0"

[analytic.storage]
type = "Local"
data_path = "/tmp/ceresdb_0"
```

Then the `CeresDB_1`'s config is:
```toml
bind_addr = "0.0.0.0"
http_port = 15440
grpc_port = 18831
log_level = "info"
enable_cluster = true

[analytic]
wal_path = "/tmp/ceresdb_1"

[analytic.storage]
type = "Local"
data_path = "/tmp/ceresdb_1"
```

### Schema&Shard declaration
Then we should define the common part -- schema&shard declaration and routing rules.

Here is the config for schema&shard declaration:
```toml
[[static_route.topology.schema_shards]]
schema = 'public_0'
[[static_route.topology.schema_shards.shard_views]]
shard_id = 0
[static_route.topology.schema_shards.shard_views.endpoint]
addr = '127.0.0.1'
port = 8831
[[static_route.topology.schema_shards.shard_views]]
shard_id = 1
[static_route.topology.schema_shards.shard_views.endpoint]
addr = '127.0.0.1'
port = 8831

[[static_route.topology.schema_shards]]
schema = 'public_0'
[[static_route.topology.schema_shards.shard_views]]
shard_id = 0
[static_route.topology.schema_shards.shard_views.endpoint]
addr = '127.0.0.1'
port = 8831
[[static_route.topology.schema_shards.shard_views]]
shard_id = 1
[static_route.topology.schema_shards.shard_views.endpoint]
addr = '127.0.0.1'
port = 18831
```

In the config above, two schemas are declared:
* `public_0` has two shards served by `CeresDB_0`.
* `public_1` has two shards served by both `CeresDB_0` and `CeresDB_1`.

### Routing rules
Provided with shcema&shard declaration, routing rules can be defined and here is an example of prefix rule:
```toml
[[route_rules.prefix_rules]]
schema = 'public_0'
prefix = 'prod_'
shard = 0
```
This rule means that all the table with `prod_` prefix belonging to `public_0` should be routed to `shard_0` of `public_0`, that is to say, `CeresDB_0`. As for the other tables whose names are not prefixed by `prod_` will be routed by hash to both `shard_0` and `shard_1` of `public_0`.

Besides prefix rule, we can also define a hash rule:
```toml
[[route_rules.hash_rules]]
schema = 'public_1'
shards = [0, 1]
```
This rule tells CeresDB to route `public_1`'s tables to both `shard_0` and `shard_1` of `public_1`, that is to say, `CeresDB0` and `CeresDB_1`. And actually this is default routing behavior if no such rule provided for schema `public_1`.

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
type = "Local"
data_path = "/tmp/ceresdb_0"

[[static_route.topology.schema_shards]]
schema = 'public_0'
[[static_route.topology.schema_shards.shard_views]]
shard_id = 0
[static_route.topology.schema_shards.shard_views.endpoint]
addr = '127.0.0.1'
port = 8831
[[static_route.topology.schema_shards.shard_views]]
shard_id = 1
[static_route.topology.schema_shards.shard_views.endpoint]
addr = '127.0.0.1'
port = 8831

[[static_route.topology.schema_shards]]
schema = 'public_0'
[[static_route.topology.schema_shards.shard_views]]
shard_id = 0
[static_route.topology.schema_shards.shard_views.endpoint]
addr = '127.0.0.1'
port = 8831
[[static_route.topology.schema_shards.shard_views]]
shard_id = 1
[static_route.topology.schema_shards.shard_views.endpoint]
addr = '127.0.0.1'
port = 18831

[[static_route.rules.prefix_rules]]
schema = 'public_0'
prefix = 'prod_'
shard = 0

[[static_route.rules.hash_rules]]
schema = 'public_1'
shards = [0, 1]
```

```toml
bind_addr = "0.0.0.0"
http_port = 15440
grpc_port = 18831
log_level = "info"
enable_cluster = true

[analytic]
wal_path = "/tmp/ceresdb_1"

[analytic.storage]
type = "Local"
data_path = "/tmp/ceresdb_1"

[[static_route.topology.schema_shards]]
schema = 'public_0'
[[static_route.topology.schema_shards.shard_views]]
shard_id = 0
[static_route.topology.schema_shards.shard_views.endpoint]
addr = '127.0.0.1'
port = 8831
[[static_route.topology.schema_shards.shard_views]]
shard_id = 1
[static_route.topology.schema_shards.shard_views.endpoint]
addr = '127.0.0.1'
port = 8831

[[static_route.topology.schema_shards]]
schema = 'public_0'
[[static_route.topology.schema_shards.shard_views]]
shard_id = 0
[static_route.topology.schema_shards.shard_views.endpoint]
addr = '127.0.0.1'
port = 8831
[[static_route.topology.schema_shards.shard_views]]
shard_id = 1
[static_route.topology.schema_shards.shard_views.endpoint]
addr = '127.0.0.1'
port = 18831

[[static_route.rules.prefix_rules]]
schema = 'public_0'
prefix = 'prod_'
shard = 0

[[static_route.rules.hash_rules]]
schema = 'public_1'
shards = [0, 1]
```

Let's call the two different config files as `config_0.toml` and `config_1.toml` but you should know in the real environment the different CeresDB intances can be deployed across different machines, that is to say, there is no need to choose different ports and data directories for different CeresDB instances so that all the CeresDB instances can share one exactly **same** config file.

## Start CeresDBs
After the configs are prepared, what we should to do is to start CeresDB container with the specific config.

Just run the commands below:
```shell
sudo docker run -d -t --name ceresdb_0 -p 5440:5440 -p 8831:8831 -v $(pwd)/config_0.toml:/etc/ceresdb/ceresdb.toml ceresdb/ceresdb-server:v0.1.0-alpha
sudo docker run -d -t --name ceresdb_1 -p 15440:15440 -p 18831:18831 -v $(pwd)/config_1.toml:/etc/ceresdb/ceresdb.toml ceresdb/ceresdb-server:v0.1.0-alpha
```

After the two containers are created and starting running, read and write requests can be served by the two-instances CeresDB cluster. 
