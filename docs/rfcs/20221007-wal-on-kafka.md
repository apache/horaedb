- Feature Name: wal implementation based on Kafka
- Tracking Issue:  https://github.com/CeresDB/ceresdb/issues/280

# Summary
The distributed wal implementation based on kafka.

# Motivation
When using CeresDB in cluster mode, a distributed wal is needed. Although a distributed wal implementation based on Oceanbase has existed but the OceanBase cluster is not so easy to build and maintain, this RFC propose to implement another distributed wal based on Kafka.

# Details
## 1. General desgin
### 1.1 Mapping
`Shard`(ceresdb) --> `topic` (kafka, with just one partition) : the amount of partitions on a single node in the kafka cluster can't be too large, otherwise it will have a bad influence on latency because the disk io becomes a complete random write. Generally, the number of partitions of 1000 magnitude on a single node is acceptable (according to [Kafka official website](https://www.confluent.io/blog/how-choose-number-topics-partitions-kafka-cluster/)). So in this implementation we choose the mapping `one shard to one partition` rather than `one table to one partition`.

By mapping in this way, we can basically limit the number of partitions on a single node to this range. The kafka `topic` will be named as: `namespace_region_{region_id}` (`region` is the concept about a table set in wal module, and now one `region` is mapped to one `shard`).
```
+------------------------+        +-----------------------+                                                               
|       Namespace        |        |        Kafka          |                                                               
|                        |        |                       |                                                               
| +--------------------+ |        | +-------------------+ |                                                               
| |      Region        | |        | |       Topic       | |                                                               
| |+------------------+| |        | |+-----------------+| |                                                               
| ||      wal         |------------->|   partition     || |                                                               
| |+------------------+| |        | |+-----------------+| |                                                               
| +--------------------+ |        | +-------------------+ |                                                               
|                        |        |                       |                                                               
| +--------------------+ |        | +-------------------+ |                                                               
| |      Region        | |        | |       Topic       | |                                                               
| |       ...          | |        | |        ...        | |                                                               
| +--------------------+ |        | +-------------------+ |                                                               
|         ...            |        |          ...          |                                                               
+------------------------+        +-----------------------+                                                               

```
### 1.2 Record Format
We choose to encode the key of WAL record with location details for independence of Kafka.

```
Key:
+---------------+----------------+-------------------+--------------------+--------------------+
| namespace(u8) | region_id(u64) |   table_id(u64)   |  sequence_num(u64) | version header(u8) |
+---------------+----------------+-------------------+--------------------+--------------------+

Value:
+--------------------+----------+
| version header(u8) | playload |
+--------------------+----------+
```
### 1.3 Data structure
The data structure for recording the wal state looks like (here just for display, thread safety is not considered yet) :

```
/// Used to record one table's log state.
struct LogState {
    /// The sequence number of the newest log entry 
    next_sequence_num: SequenceNumber,
    /// The kafka offset in its partition of the newest log entry 
    cur_offset: Option<KafkaOffset>,
    /// The start kafka offset of current log set 
    /// which is still not deleted(due to compaction)
    start_offset: Option<KafkaOffset>,
}

/// All tables in the same shard will be managed by a `Region`.
struct Region {
    ...
    log_states: HashMap<TableId, LogState>,
    ...
}
```
## 2. Main process
### 2.1 Open namespace
- Fetch all topics and filter them according to the namespace, and cache the results.
### 2.2 Open region
- Search the region in the opened namespace. 
- If the region not found and auto creating is defined, create the corresponding topic in kafka and add it to cache.
- Return the found or created `region`.
### 2.3 Write
- Open the corresponding `region` (auto create it if necessary).
- Put the log batch to `wal partition`.
- Update `cur_offset` and `next_sequence_number` in memory.
### 2.4 Read
- Open the corresponding `region`.
- Just read all the logs of the `region`, and the split and replay work will be done by the caller.
### 2.5 Delete
Log's deletion can be split to two steps:
+ Mark the delete offset.
+ Do delayed deletion periodically in a background thread.
#### Mark
+ Each table's `cur_sequence_num` and `cur_offset` will be updated in each log writing, and `start_offset` will be updated in its first log writing.
+ When `mark_delete_entries_up_to` is called on a table, it will make `start_offset` None.
#### Delete
The deletion logic done in a background thread called `cleaner`, and it may be a bit complicated. In the `cleaner`'s checking, two situations should be taken into cosiderations:
- Common cleaning:
  - Scan all `log_states` in region,  check their `start_offset` and get their maximum `cur_offset`(or directly get it from kafka?) .
  - If all the start_offsets are None, it represents that every table is just compacted, we will delete records up to maximum `cur_offset`.
  - Otherwise, we will delete records up to the minimum `start_offset`.
- Too large wal size (while some tables have low update frequency or are never updated for a long time) :
  - We should get such tables and flush them, and try to clean the wals afterwards.
  - But the strategy about this still needs more consideration and discussion, the simplest way to avoid this situation is to flush all tables periodically.

# Drawbacks
Due to wals of all tables in a region will be written together, the splitting work is needed while replay, which leads to complicated replay and possible poor performance.

# Alternatives
Hbase can be a choice, too. But it is a bit complicated to build and maintain because Hbase depends on HDFS.
