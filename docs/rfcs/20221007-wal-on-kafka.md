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
### 1.3 Metadata
The most important and hardest thing is to design the metadata for recording the wal state of region.
The metadata is designed as following(it is just pseudocode for display here, implementation details(such as thread safety) are not considered yet) :

```
/// Used to record one table's log state.
struct RegionMeta {
  table_metas: HashMap<TableId, Table>
}

struct TableMeta {
    /// The sequence number of the newest log entry 
    next_sequence_num: SequenceNumber,
    
    /// The kafka offset in its partition of the newest log entry 
    /// The start kafka offset of current log set 
    /// which is still not deleted(due to compaction)
    latest_marked_deleted: SequenceNumber,
    
    /// The high watermark after this table's latest writing.
    current_high_watermark: Offset,

    /// Map the sequence numbers to offsets in every write.
    ///
    /// It will be removed to the mark deleted sequence number after flushing.
    sequence_offset_mapping: BTreeMap<SequenceNumber, Offset>,
}
```
The `region meta snapshot`(make from `RegionMeta`) needed to be created and sync to Kafka before cleaning logs, that is important for correct region recovering.

## 2. Main process
### 2.1 Open namespace
- Fetch all topics and filter them according to the namespace, and cache the results.
### 2.2 Open region
- Search the region in the opened namespace. 
- If the region found, we need to recover its metadata, the recovering can be split to two steps:
  - Recover from `region meta snapshot` mentioned above.
  - Recover from logs written after `region meta snapshot`.
- If the region not found and auto creating is defined, create the corresponding topic in Kafka.
- Add the found or created `region` to cache, return it afterwards.
### 2.3 Write
- Open the corresponding `region` (auto create it if necessary).
- Put the log batch to `wal partition`.
- Update `next_sequence_num`, `current_high_watermark`, `sequence_offset_mapping` in `TableMeta`.
### 2.4 Read
- Open the corresponding `region`.
- Just read all the logs of the `region`, and the split and replay work will be done by the caller.
### 2.5 Delete
Log's deletion can be split to two steps:
+ Mark the deleted offset.
+ Do delayed cleaning work periodically in a background thread.
#### Mark
+ Update `latest_marked_deleted` and `sequence_offset_mapping`(remove the entries to `latest_marked_deleted`) in `TableMeta`.
+ Maybe we need to make and sync the `region meta snapshot` to Kafka while dropping table.
#### Clean
The deletion logic done in a background thread called `cleaner`:
- Make `region meta snapshot`.
- Decide whether needed to clean logs based on `region meta snapshot`.
- If so, sync the `region meta snapshot` to Kafka first, and clean logs afterwards.

# Drawbacks
Due to wals of all tables in a region will be written together, the splitting work is needed while replay, which leads to complicated replay and possible poor performance.

# Alternatives
Hbase can be a choice, too. But it is a bit complicated to build and maintain because Hbase depends on HDFS.
