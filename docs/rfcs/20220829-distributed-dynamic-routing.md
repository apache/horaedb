- Feature Name: distributed-dynamic-routing
- Tracking Issue: [#237](https://github.com/CeresDB/ceresdb/issues/237)

# Summary
Support distributed dynamic routing for CeresDB cluster mode for failover and load balancing.

# Motivation
In Release v0.3, we initially implemented the cluster mode of CeresDB and provided static routing based on CeresMeta's initialization.
However, it is obvious that this simple implementation cannot support the dynamic scheduling in distributed scenarios.
Through distributed dynamic routing, we hope to achieve：

- High availability.
- Automatic scheduling of CeresDB computing resources for good load balancing.

# Details
### Cluster Mode Architecture
```
                                               ┌──────────────────┐                                                  
                           ┌───────────────────┤CeresMeta Cluster ├────────────────────┐                             
                           │                   └──────────────────┘                    │                             
                           │        ┌────────┐      ┌────────┐       ┌────────┐        │                             
                           │        │        │      │        │       │        │        │                             
                           │        │  etcd  │      │  etcd  │       │  etcd  │        │                             
                           │        │        │      │        │       │        │        │                             
                           │        └────────┘      └────────┘       └────────┘        │                             
                           └─────────────────────────┬──────▲──────────────────────────┘                             
                                                     │      │                                                        
                                            ┌────────┴──────┴────────┐                                               
                                            │   Routing Infomation   │                                               
                                            └────────┬──────┬────────┘                                               
                                              ┌──────▼──────┴──────┐                                                 
                           ┌──────────────────┤  CeresDB Cluster   ├────────────────────┐                 ┌─────────┐
                           │                  └────────────────────┘                    │                 │         │
                           │                                                            │  ┌────────────┐ │         │
                           │    ┌───────┐                     ┌───────┐                 │  │Upload/Fetch│ │         │
              ┌──────────┐ │  ┌─┤ Node0 ├───────────────┐   ┌─┤ Node1 ├───────────────┐ │  │    SST     │ │         │
              │Write/Read│ │  │ └───────┘               │   │ └───────┘               │ │  └────────────┘ │         │
┌──────────┐  │ Request  │ │  │ ┌────────┐  ┌────────┐  │   │ ┌────────┐  ┌────────┐  │ ├─────────────────▶         │
│          │  └──────────┘ │  │ │Shard0 L├┐ │Shard1 F├┐ │   │ │Shard0 F├┐ │Shard1 L├┐ │ │                 │ Remote  │
│  Client  │───────────────▶  │ ├────────┘│ ├────────┘│ │   │ ├────────┘│ ├────────┘│ │ │                 │ Storage │
│          │  ┌──────────┐ │  │ │ Table0  │ │ Table2  │ │   │ │ table0  │ │ table2  │ │ ◀─────────────────┤         │
└──────────┘  │ Routing  │ │  │ │ Table1  │ │ Table3  │ │   │ │ table1  │ │ table3  │ │ │                 │         │
              │Infomation│ │  │ │         │ │         │ │   │ │         │ │         │ │ │                 │         │
              └──────────┘ │  │ └─────────┘ └─────────┘ │   │ └─────────┘ └─────────┘ │ │                 │         │
                           │  │                         │   │                         │ │                 │         │
                           │  └─────────────────────────┘   └─────────────────────────┘ │                 │         │
                           │                                                            │                 │         │
                           └────────────────────────────────────────────────────────────┘                 └─────────┘                                                                       
                                                                                                                     
                                                                                                                     
                                                                                                              
```
Explain：

- Shard: CeresDB data shard, one shard consists of a batch of tables. Shard's roles are divided into leader and follower, only the leader is responsible for writing requests.
- Node: CeresDB Compute Node, one node is responsible for processing multiple Shards.
- Table: Basic scheduling unit of CeresDB cluster, CeresDB implements scheduling of cluster by scheduling tables.
- Routing: Routing relationship from table to endpoint, the client relies on it to send requests to the correct node.
- CeresMeta: An independently deployed service for managing and scheduling CeresDB cluster.

### Metadata Definition
#### ShardRole
```rust
pub enum ShardRole {
    LEADER = 0,
    FOLLOWER = 1,
    PENDING_LEADER = 2,
    PENDING_FOLLOWER = 3
}

// LEADER has RW permission
// FOLLOWER has R permission
// PENDING_LEADER is the follower who will become the leader in the process of leader switching，It has R permission
// PENDING_FOLLOWER is the leader who will become the follower in the process of leader switching，It has R permission
```
```
┌────┐                   ┌────┐          
│ RW ├─────────┐         │ R  ├─────────┐
├────┘         │         ├────┘         │
│    Leader    ◀─────────│PendingLeader │
│              │         │              │
└───────┬──────┘         └───────▲──────┘
        │                        │       
┌────┐  │                ┌────┐  │       
│ R  ├──▼──────┐         │ R  ├──┴──────┐
├────┘         │         ├────┘         │
│   Pending    ├─────────▶   Follower   │
│   Follower   │         │              │
└──────────────┘         └──────────────┘
```

### Scheduling strategy
#### Shard Load Balance
The simple load balancing strategy based on shard,  only ensures that the number of shards distributed on each computing node is balanced, and cannot guarantee that the actual load of the computing node is balanced.

- The implementation is simple, only the routing relationship between shard and computing nodes is processed, and the routing relationship between table and shard is randomly allocated
- Real load balancing cannot be realized. Some computing nodes may become hot spots due to large tables, and need to manually handle the routing from table to shard.
```
                                                ┌───────┐                                                
   ┌──────┐        ┌──────┐         ┌──────┐    │       │    ┌──────┐        ┌──────┐         ┌──────┐   
┌──┤Node0 ├──┐  ┌──┤Node1 ├──┐   ┌──┤Node2 ├──┐ │ Shard │ ┌──┤Node0 ├──┐  ┌──┤Node1 ├──┐   ┌──┤Node2 ├──┐
│  └──────┘  │  │  └──────┘  │   │  └──────┘  │ │ Load  │ │  └──────┘  │  │  └──────┘  │   │  └──────┘  │
│┌─────────┐ │  │┌─────────┐ │   │┌─────────┐ │ │Balance│ │┌─────────┐ │  │┌─────────┐ │   │┌─────────┐ │
││Shard0 L │ │  ││Shard0 F │ │   ││Shard1 L │ │ │       │ ││Shard0 L │ │  ││Shard0 F │ │   ││Shard1 L │ │
│└─────────┘ │  │└─────────┘ │   │└─────────┘ │ └───────┘ │└─────────┘ │  │└─────────┘ │   │└─────────┘ │
│┌─────────┐ │  │┌─────────┐ │   │            │──────────▶│┌─────────┐ │  │┌─────────┐ │   │┌─────────┐ │
││Shard2 L │ │  ││Shard1 F │ │   │            │           ││Shard2 L │ │  ││Shard1 F │ │   ││Shard2 F │ │
│└─────────┘ │  │└─────────┘ │   │            │           │└─────────┘ │  │└─────────┘ │   │└─────────┘ │
│            │  │┌─────────┐ │   │            │           │            │  │            │   │            │
│            │  ││Shard2 F │ │   │            │           │            │  │            │   │            │
│            │  │└─────────┘ │   │            │           │            │  │            │   │            │
└────────────┘  └────────────┘   └────────────┘           └────────────┘  └────────────┘   └────────────┘
```



### Shard Change Process
In CeresDB, we could change `Replication_Factor` to determine the number of shard replication. Therefore, in the processing of shard, we also need to consider the number of shard replication



#### Switch Leader

Only Leader Shard(`Replication_Factor` == 1)

1. When there is only one leader shard, there is no need to switch leader, CeresMeta only need to migrate shard to another node.


Multi Follower Shard(`Replication_Factor` >= 2)

1. First, set the original leader shard and the follwer to be switched to PENDING_ FOLLOWER and PENDING_ LEADER.
1. Wait for the leader shard to complete the preparatory action before the leader switch, and notify the CeresMeta after the completion. In this process, the new leader cannot process the write request.
1. After receiving this request, CeresMeta will update the status of the two shards, and the leader switch is completed.


#### Shard Migrate

Only Leader Shard(`Replication_Factor` == 1)
1. Same as `Multi Follower Shard` leader shard.


Multi Follower Shard(`Replication_Factor` >= 2)
- Leader Shard：
    1. First, CeresMeta adjusts the routing relationship and create a new follower shard on the node.
    1. Follower shard notifies CeresMeta after initialization.
    1. CeresMeta started the process of leader switch, switched the leader to the newly created follower shard.
    1. Close the original leader shard after the leader switch is completed.
- Follower Shard：
    1. Adjust shard node mapping to directly assign this shard to another node.


#### Shard Split

Only Leader Shard(`Replication_Factor` == 1)

1. CeresMeta creates a new shard and distributes it to the node with the lowest load.
1. After the new shard is created, the table routing relationship in the shard is processed, and a part of the table is split to the new shard according to certain rules. (This is the same as shard migration)

Pending Problem：

1. How to decide which tables need to be split into new shards?

Conclusion:

1. This depends on the actual scheduling strategy of CeresDB and is not within the scope of this RFC. We will discuss it separately later


Multi Follower Shard(`Replication_Factor` >= 2)

1. The process of splitting a single shard is repeated in parallel until each shard is successfully split.


#### Shard Merge
Only Leader Shard(`Replication_Factor` == 1)

1. If the shards to be merged is not on the same node, migrate one to same node first. (The migration process is the same as above)
2. Shard is only a logical scheduling concept, and CeresDB nodes do not actually perceive it, so we can directly merge two shards into one in CeresMeta, which will not cause any additional cost.

Multi Follower Shard(`Replication_Factor` >= 2)

1. Same as `shard split`, repeat the merging of individual shards until each one succeeds.


### Processing flow
#### Cluster registration and initialization

1. CeresMeta received the request for cluster registration, create cluster CeresMeta info and save in etcd, set the ClusterTopologyState is EMPTY.
1. Waiting for node regisration, When the number of registered nodes has reached minimum number required by the cluster, the cluster initialization process will start.
1. Initialize the shard routing topology and distribute the shard evenly on each node.
1. Set the ClusterTopologyState to STABLE, initialization complete, after that, the cluster can normally provide external services.

#### Create Table

1. The client initiates a create table request. In RPC mode, it will access the specified endpoint returned by CeresmMeta. In HTTP mode, it will randomly access an available endpoint.
1. After receiving the request, the node will complete the table creation operation and notify CeresMeta. All subsequent reads and writes to this table will be processed by the node. CeresMeta will modify the routing relationship between the table and the shard and associate this table with the shard of the current node.
1. The routing relationship of the new table has been registered. Now the table can be read and written to the outside normally.

#### Write / Read Table

1. Client accesses the endpoint specified by CeresMeta through RPC. Because the client caches the routing relationship, it may be expired.
1. After receiving the request, the node needs to decide whether to process the request normally according to the latest routing table.
    - Shard is in normal status and handles requests normally.
    - The current shard is no longer on this node. The node returns an expiration flag. The client re initiates the route request to refresh the routing relationship.

# Drawbacks
#### Table Split
CeresDB's current minimum scheduling unit is table. It does not support splitting or merging table. It can only migrate tables between nodes. In extreme cases, a single table may be too large for a single node to handle.

# Alternatives
#### Remove Shard
In the current design, the concept of shard adds a lot of complexity, and it seems to have no obvious effect at present. Direct scheduling table can also achieve the same function. Maybe shard can be removed in the future?
#### More complex scheduling
In the current design of scheduling, we only implement load balancing based on table cost. In the future, we can refer to the design of PD scheduler to implement more kinds of schedulers.
