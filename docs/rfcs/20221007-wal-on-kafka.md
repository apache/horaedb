- Feature Name: wal implementation based on kafka
- Tracking Issue:  https://github.com/CeresDB/ceresdb/issues/280

# Summary
The distributed wal implementation based on kafka.

# Motivation
When using ceresdb of cluster mode, a distributed wal is needed. Although a distributed wal implementation based on oceanbase has existed but the oceanbase cluster is not so easy to build and maintain, I proposal to implement another distributed wal based on kafka.

# Details
## 1. General desgin
### 1.1 Mapping
`Shard`(ceresdb) --> `topic` (kafka, with just one partition) : the amount of partitions on a single node in the kafka cluster can't be too large, otherwise it will have a great impact on latency (the write disk becomes a complete random write). Generally, the number of partitions of 1000 magnitude on a single node is acceptable (according to [Kafka official website](https://www.confluent.io/blog/how-choose-number-topics-partitions-kafka-cluster/)). So in this implementation we choose the mapping `one shard to one partition` rather than `one table to partition`.

By mapping like this, we can basically limit the number of partitions on a single node to this range. The kafka `topic` will be named like this: `namespace_region_x` (`region` is the concept about table set in wal module, now one `region` is mapped to one `shard`).
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
Wal record, we choose the key with detail location information for using such record format in not only the implementation on kafka.

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
The data structure used to record above information is like this (just use to display, thread safe is not considered yet) : 

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
- Fetch all topics and filter them according to the namespace prefix. Cache the results after.
### 2.2 Open region
- Search the region in opened namespace. 
- If not found and auto creating is defined,  create corresponding topic in kafka and add it to cache.
- Return the pointer of `region`.
### 2.3 Write
- Open the corresponding `region` (define auto creating).
- Put the log batch to `wal partition`.
- Update `cur_offset` and `next_sequence_number` in memory.
### 2.4 Read
- Open the corresponding `region`.
- Just read all the logs of the `region`, the split and replay work will be done on the high level.
### 2.5 Delete
Log's deletion can be split to two steps:
+ Mark the delete offset.
+ Do delayed deletion periodically in a cleaner thread.
#### Mark
+ Each table's `cur_sequence_num` and `cur_offset` will be updated in each log writing, and `start_offset` will be updated in its first log writing.
+ When `mark_delete_entries_up_to` is called by a table, it will make `start_offset` None.
#### Delete
The deletion logic done in a cleaner thread, and it may be a bit complicated. In cleaner's checking, I think logs should be considered to delete in these two situation:
- Common cleaning:
  - Scan all `log_states` in region,  check their `start_offset` and get their maximum `cur_offset`(or directly get it from kafka?) .
  - If all the start_offsets are None, it represents that every table is just compacted, we will delete records up to maximum `cur_offset`.
  - Otherwise, we will delete records up to the minimum `start_offset`.
- Too large wal size (while some tables have low update frequency or are never updated for a long time) :
  - We should get such tables and flush them, and try to clean the wals after.
  - But the strategy about this still needs more consideration and discussion, the simplest way to avoid this situation is to flush all tables periodically.

# Drawbacks
Due to wals of all tables in a region will be written together, the splitting work is needed while replay, which leads to complicated replay and possible lower replay performance.

# Alternatives
Hbase can be a choice, too. But it is a bit complicated to build and maintain because Hbase depends on HDFS.
