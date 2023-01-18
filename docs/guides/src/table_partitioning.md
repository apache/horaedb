# Table Partitioning
This chapter discusses `PartitionTable`.  
The partition table syntax used by ceresdb is similar to that of mysql.  
General partition tables include `Range Partitioning`, `List Partitoning`, `Hash Partitioning`, and `Key Partititioning`.  
CeresDB currently only supports `Key Partititioning`.
Refer to [mysql](https://dev.mysql.com/doc/refman/8.0/en/partitioning-types.html)

## architecture
Similar to mysql, different portions of a table are stored as separate tables in different locations.  
Currently designed, a partition table can be opened on multiple ceresdb nodes, supports writing and querying at the same time, and can be expanded horizontally.  
As shown in the figure below, `PartitionTable` is opened on node0 and node1, and the physical subtables where the actual data are stored on node2 and node3.
```
                                       ┌───────────────────────┐     ┌───────────────────────┐           
                                       │Node0                  │     │Node1                  │           
                                       │   ┌────────────────┐  │     │   ┌────────────────┐  │           
                                       │   │ PartitionTable │  │     │   │ PartitionTable │  │           
                                       │   └────────────────┘  │     │   └────────────────┘  │           
                                       │            ┬          │     │                       │           
                                       └────────────┼──────────┘     └───────────────────────┘           
                                                    │                                                    
                                                    │                                                    
             ┌───────────────────────┬──────────────┴──────────────┬───────────────────────┐             
             │                       │                             │                       │             
┌────────────┼───────────────────────┼─────────────┐ ┌─────────────┼───────────────────────┼────────────┐
│Node2       │                       │             │ │Node3        │                       │            │
│            ▼                       ▼             │ │             ▼                       ▼            │
│ ┌─────────────────────┐ ┌─────────────────────┐  │ │  ┌─────────────────────┐ ┌─────────────────────┐ │
│ │                     │ │                     │  │ │  │                     │ │                     │ │
│ │     SubTable_0      │ │     SubTable_1      │  │ │  │     SubTable_2      │ │     SubTable_3      │ │
│ │                     │ │                     │  │ │  │                     │ │                     │ │
│ └─────────────────────┘ └─────────────────────┘  │ │  └─────────────────────┘ └─────────────────────┘ │
│                                                  │ │                                                  │
└──────────────────────────────────────────────────┘ └──────────────────────────────────────────────────┘
```

## Key Partitioning
`Key Partitioning` supports one or more column calculations, using the hash algorithm provided by ceresdb for calculations.
Use restrictions:
* Only tag is supported as partition key
* `LINEAR KEY` is not supported yet.
```sql
CREATE TABLE `demo`(
    `name`string TAG,
    `id` int TAG, 
    `value` double NOT NULL, 
    `t` timestamp NOT NULL, 
    TIMESTAMP KEY(t)
    ) PARTITION BY KEY(name) PARTITIONS 2 ENGINE = Analytic
```
Refer to [mysql key partitioning](https://dev.mysql.com/doc/refman/5.7/en/partitioning-key.html).

## Query
Since the partition table data is actually stored in different physical tables, it is necessary to calculate the actual requested physical table according to the query request when querying.  
The implementation of the partition table is in [PartitionTableImpl](https://github.com/CeresDB/ceresdb/blob/89dca646c627de3cee2133e8f3df96d89854c1a3/analytic_engine/src/table/partition.rs).
The query will calculate the physical table to be queried according to the query parameters,   
and then remotely request the node where the physical table is located to obtain data through the ceresdb internal service [remote engine](https://github.com/CeresDB/ceresdb/blob/89dca646c627de3cee2133e8f3df96d89854c1a3/server/src/grpc/remote_engine_service/mod.rs) (support predicate pushdown).

`Key partitioning` currently supports the following request calculations:
* The keywords `and` and `or` are supported.
* The keyword `in` is supported.
* The use of `=` is supported.

Using `>` `<` will scan all physical tables.
`Key partitioning` rule is implemented in [KeyRule](https://github.com/CeresDB/ceresdb/blob/89dca646c627de3cee2133e8f3df96d89854c1a3/table_engine/src/partition/rule/key.rs)

## Write
The write process is similar to the query process.  
First, according to the partition rules, the write request is split into different partitioned physical tables, and then sent to different physical nodes through the remote engine for actual data writing.