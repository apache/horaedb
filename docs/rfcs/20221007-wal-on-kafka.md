- Feature Name: (fill me in with a unique ident, my_awesome_feature)
- Tracking Issue:  https://github.com/CeresDB/ceresdb/issues/280

# Summary
The distributed wal implementation based on kafka.

# Motivation
When using ceresdb of cluster mode, a distributed wal is needed. Although a distributed wal implementation based on oceanbase has existed but the oceanbase cluster is not so easy to build and maintain, I proposal to implement another distributed wal based on kafka.

# Details
## 1. General desgin
### 1.1 Mapping
+ `Shard` (ceresdb) --> `region` (ceresdb):  shardis used to describe a set of tables which will be scheduled by ceresmeta, andregionis used to describe a set of wals which will be written to the same storage unit (for example, a file). Generally, write performance will be not so good If too many storage units are used, so we made such design with reference to Hbase.
+ `Region`(ceresdb) --> `topic` (kafka, with just `wal parition` and `meta partition`) : the amount of partitions on a single node in the kafka cluster can't be too large, otherwise it will have a great impact on latency (the write disk becomes a complete random write). Generally, the number of partitions of 1000 magnitude on a single node is acceptable (Huawei Cloud, Didi, Kafka official website). By mapping like this, we can basically limit the number of partitions on a single node to this range. The kafka topic will be named like this: `namespace_region_x`.
```
+------------------------+         +-----------------------+
|       Namespace        |         |        Kafka          |
|                        |         |                       |
| +--------------------+ |         | +-------------------+ |
| |      Region        | |         | |       Topic       | |
| |+------------------+| |         | |+-----------------+| |
| ||   Region meta    |-------------->| Meta partition  || |
| |+------------------+| |         | |+-----------------+| |
| |+------------------+| |         | |+-----------------+| |
| ||   Region wal     |-------------->|  Wal partition  || |
| |+------------------+| |         | |+-----------------+| |
| +--------------------+ |         | +-------------------+ |
|                        |         |                       |
| +--------------------+ |         | +-------------------+ |
| |      Region        | |         | |       Topic       | |                                                              
| |       ...          | |         | |        ...        | | 
| +--------------------+ |         | +-------------------+ |                                                              
|         ...            |         |          ...          |
+------------------------+         +-----------------------+
```
### 1.2 Record Format
Meta record, used to mark the latest start sequence number :
```
Key:
+----------------------+---------------+
|  version header(u8)  | table id(u32) |
+----------------------+---------------+

Value:
+--------------------+----------------------------+
| version header(u8) | start sequence number(u64) |
+--------------------+----------------------------+
```
Wal record, `is_start` field is needed because following situation can happen:

![is_start](https://user-images.githubusercontent.com/34352236/194684993-64574f4c-ff39-42fe-aac0-83e20a75c484.svg)

```
Key:
+-------------------+----------------+----------------------+-------------+
| version header(u8)|  table id(u32) | sequence number(u64) | is start(u8)|
+-------------------+----------------+----------------------+-------------+

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
- If not found and auto creating is defined,  create correspoding topic in kafka and add it to cache.
- Return the pointer of `region`.
### 2.3 Write
- Open the corresponding `region` (define auto creating).
- Put the log batch to `wal partition`.
- Update `cur_offset` and `next_sequence_number` in memory.
### 2.4 Read
- Open the corresponding `region`.
- The actual logs fetching work will be done by a fetcher thread.
- Fetcher thread will be triggered by the first table which reads logs, and each table will be assigned a channel to wait its logs after.
- The fetcher thread will do two things as follow:
  - Firstly, read all logs from `meta partition` and get their `start_sequence_num`s, all logs belongs to a table but with sequence nums smaller than `start_sequence_num` will be filtered. 
  - Fetch logs from `wal partition` from `region topic` and put them to corresponding channel.
### 2.5 Delete
Log's deletion can be splitted to two steps:
+ Mark the delete offset.
+ Do delayed deletion periodically in a cleaner thread.
#### Mark
+ Each table's `cur_sequence_num` and `cur_offset` will be updated in each log writing, and `start_offset` will be updated in its first log writing.
+ When `mark_delete_entries_up_to` is called by a table, it will make `start_offset` None.
+ Put a record with its `table_id` and `next_sequence_num` to corresponding `meta partition` in `region topic`. Error while putting is tolerable, it will just make the replay time longer.
#### Delete
The deletion logic done in a cleaner thread, and it may be a bit complicated. In cleaner's checking, I think logs should be considered to delete in these two situation:
- Common cleaning:
  - Scan all `log_states` in region,  check their `start_offset` and get their maximum `cur_offset`(or directly get it from kafka?) .
  - If all the start_offsets are None, it represents that evey table is just compacted, we will delete records up to maximum `cur_offset`.
  - Otherwise, we will delete records up to the minimum `start_offset`.
- Too large wal size (while some tables have low update frequency or are never updated for a long time) :
  - We should get such tables and flush them, and try to clean the wals after.
  - But the stategy about this still needs more consideration and discussion, the simplest way is to flush all tables in the region.

# Drawbacks
Due to wals of all tables in a region will be written together, the splitting work is needed while replaying, which leads to complicated replaying and possible lower replaying performance.

# Alternatives
Hbase can be a choice, too. But it is a bit complicated to build and maintain because Hbase depends on HDFS.
