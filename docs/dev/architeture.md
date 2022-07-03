# Introduction to CeresDB's Architecture
## Target
- Provide the overall cognition of CeresDB to the developers who want to know more about CeresDB but have no way to start.
- Make a brief introduction to the important modules of CeresDB and the connections between these modules but details about their implementations are not be involved.

## Motivation
CeresDB is a timeseries database. However, CeresDB's goal is to handle both timeseries and analytic workloads compared with the traditional ones, which always have a poor performance in handling analytic workloads.

In the traditional timeseries database, the indexed columns (InfluxDB calls them `Tag`, Prometheus calls them `Label` and this document calls them `Tag`) are normally indexed by generating an inverted index. However, it is found that the cardinality of `Tag` varies in different scenarios. Some scenarios where the cardinality is very high directly leads to a very high cost in storing and querying inverted index. However, the scanning+pruning often used by the analytical databases can do a good job to handle such a scenarios.

The basic design idea of CERESDB is to adopt a hybrid storage format and the corresponding query method for a better performance in processing timeseries and analytic workloads.

## Architure
```console
┌────────────────────────────────────────────────────┐
│                                                    │
│            RPC Layer (HTTP/gRPC/MySQL)             │
│                                                    │
└────────────────────────────────────────────────────┘
┌────────────────────────────────────────────────────┐
│                  Query Protocols:                  │
│                   SQL/PromQL/...                   │
│                                                    │
└────────────────────────────────────────────────────┘
┌────────────────────────────────────────────────────┐
│                                                    │
│         Catalog/Schema/Table Management            │
│                                                    │
└────────────────────────────────────────────────────┘
┌────────────────────────────────────────────────────┐
│                                                    │
│                   Query Engine                     │
│    ┌─────────────────┐    ┌──────────────────┐     │
│    │     Parser      │    │     Planner      │     │
│    └─────────────────┘    └──────────────────┘     │
│                                                    │
│    ┌─────────────────┐    ┌──────────────────┐     │
│    │    Optimizer    │    │     Executor     │     │
│    └─────────────────┘    └──────────────────┘     │
│                                                    │
└────────────────────────────────────────────────────┘
┌────────────────────────────────────────────────────┐
│                                                    │
│                                                    │
│              Pluggable Table Engine                │
│     ┌─────────────────────────────────────────┐    │
│     │                Analytic                 │    │
│     │  ┌────────────────┐┌────────────────┐   │    │
│     │  │      Wal       ││    Memtable    │   │    │
│     │  └────────────────┘└────────────────┘   │    │
│     │  ┌────────────────┐┌────────────────┐   │    │
│     │  │     Flush      ││   Compaction   │   │    │
│     │  └────────────────┘└────────────────┘   │    │
│     │  ┌────────────────┐┌────────────────┐   │    │
│     │  │    Manifest    ││  Object Store  │   │    │
│     │  └────────────────┘└────────────────┘   │    │
│     └─────────────────────────────────────────┘    │
│                                                    │
│       ┌ ─ ─ ─ ─ ─ ─ ─       ┌ ─ ─ ─ ─ ─ ─ ─        │
│            Hybrid    │         Timeseries  │       │
│       └ ─ ─ ─ ─ ─ ─ ─       └ ─ ─ ─ ─ ─ ─ ─        │
│                                                    │
│                                                    │
└────────────────────────────────────────────────────┘
```

The figure above shows the architecture of CERESDB stand-alone service and details of some important modules will be described in the following part.

### RPC Layer
[module path](https://github.com/CeresDB/ceresdb/tree/main/server)

The current RPC supports multiple protocols including HTTP, GRPC, MySQL.

Basically, HTTP and MySQL are used to debug CeresDB, manually query and perform DDL operations (such as creating, deleting tables, etc.). And gRPC protocol can be regarded as a customized protocol for high-performance, which is suitable for massive reading and writing operations.

### Query Protocol
[module path](https://github.com/CeresDB/ceresdb/tree/main/sql)

Now only the SQL is supported. In the short term, we will support PromQL. In the future, some other query protocols may be supported too.

### Metadata management
[module path](https://github.com/CeresDB/ceresdb/tree/main/catalog_impls)

The metadata level adopted by CERESDB is similar to PostgreSQL: `Catalog > Schema > Table`, but they are only used as namespace.

At present, `Catalog` and `Schema` have two different kinds of implementation for stand-alone and distributed mode because some strategies to generate ids and ways to persist meta data are different in different mode.

### Query Engine
[module path](https://github.com/CeresDB/ceresdb/tree/main/query_engine)

`Query Engine` is responsible for creating, optimizing and executing query plan given a SQL and now is mainly delegated to [DataFusion](https://github.com/apache/arrow-datafusion).

Besides the basical functions of SQL, `CeresDB` also utilizes the extensibility provided by [DataFusion](https://github.com/apache/arrow-datafusion) for some customized query protocols and optimization rules for query plan. For example, the implementation of `PromQL` is implemented in this way and read related implementation if you are interested in these.

### Pluggable Table Engine
[module path](https://github.com/CeresDB/ceresdb/tree/main/table_engine)

`Table Engine` is actually a storage engine for managing tables in `CeresDB` and the pluggability of `Table Engine` is a core design of `CeresDB` which makes a different in achieving the target (process both timeseries and analytic workloads well). `CeresDB` will own multiple kinds of `Table Engine` for different workloads and the most appropriate one should be chosen as the storage engine according to the workload pattern.

Now the requirements for a `Table Engine` are:
- Manage all the shared resources under the engine
 - Memory
 - Storage
 - CPU
- Manage metadata of tables such as table schema and table options
- Provide `Table` instances which can read and write data
- Take responsibilities for creating, opening, deleting, and closing `Table` instance
- ....

The things that a `Table Engine` needs to process are a little complicated. And now in `CeresDB` only one `Table Engine` called `Analytic` is provided and does a good job in processing analytical workload but it is not ready yet to handle the timeseires database (we plan to enhance it for a better performance by increasing some indexes which helps handle timeseries workload).

The following part gives a description about details of `Analytic Table Engine`.

#### WAL
[module path](https://github.com/CeresDB/ceresdb/tree/main/wal)

The model of `CeresDB` processing data is `WAL + MemTable` that the recent written data is written to `WAL` first and then to `MemTable` and after a certain amount of data in `MemTable` is accumulated, the data will be organized in a query-friendly form to persistent devices.

Now two implemenations of `WAL` are provided for stand-alone and distributed mode:
- For stand-alone mode, `WAL` is based on `RocksDB` and data is persisted in the local disk.
- For distributed mode, `WAL` is requred as a distributed component and to be responsible for reliability of the newly written data so now we provide a implementation based on [OceanBase](https://github.com/oceanbase/oceanbase) and in our roadmap a more lightweight implementation will be provided.

Besides, `WAL`'s trait definition tells that WAL has the concept of `Region` and actually each table is assigned to a `Region` so that the independence between tables is gained and such an indepenence provides convenience for some operations on table's level (such as different TTLs for different tables).

#### MemTable
[module path](https://github.com/CeresDB/ceresdb/tree/main/analytic_engine/src/memtable)

`Memtable` is used to store the newly written data and after a certain amount of data is accumulated, `CeresDB` organizes the data in `MemTable` into a query-friendly storage format (SST) and stores it to the persistent device. Before persisting, `MemTable` also provides querying.

The current implementation of `MemTable` is based on [agatedb's skiplist](https://github.com/tikv/agatedb/blob/8510bff2bfde5b766c3f83cf81c00141967d48a4/skiplist). It allows concurrent reads and writes and equipped with [Arena](https://github.com/CeresDB/ceresdb/tree/main/components/skiplist) has the capability of controlling memory usage.

#### Flush
[module path](https://github.com/CeresDB/ceresdb/blob/main/analytic_engine/src/instance/flush_compaction.rs)

What the flushing does is that when the memory usage of `MemTable` reaches the threshold, some `MemTables` are selected for flushing into query-friendly SSTs saved on persistent device.

During the flushing procedure, the data will be divided by a centern time range (which is configured by table option `Segment Duration`), and no SST will span the `Segment Duration`. Actually this is also a common operation in most timeseries databases which organizes data according in the time dimension to speed up subsequent time-related operations, such as querying data over a time range and assisting purge data outside the `TTL`.

At present, the control process of flush is a little complicated, so the details will be explained in another document.

#### Compaction
[module path](https://github.com/CeresDB/ceresdb/tree/main/analytic_engine/src/compaction)

The data of `MemTable` is flushed as SSTs, but the file size of newly flushed SST may be very small. And too small or too many SSTs lead to the poor query performance. Therefore, compaction is then introduced to rearrange the SSTs so that the multiple smaller SST files can be compacted into a larger SST file.

The detailed strategy of compaction will also be described with flush in subsequent documents.

