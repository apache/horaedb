# Introduction to CeresDB's Architecture
## Target
- Provide the overview of CeresDB to the developers who want to know more about CeresDB but have no idea where to start.
- Make a brief introduction to the important modules of CeresDB and the connections between these modules but details about their implementations are not be involved.

## Motivation
CeresDB is a timeseries database. However, CeresDB's goal is to handle both timeseries and analytic workloads compared with the traditional ones, which usually have a poor performance in handling analytic workloads.

In the traditional timeseries database, the `Tag` columns (InfluxDB calls them `Tag` and Prometheus calls them `Label`) are normally indexed by generating an inverted index. However, it is found that the cardinality of `Tag` varies in different scenarios. And in some scenarios the cardinality of `Tag` is very high, and it takes a very high cost to store and retrieve the inverted index. On the other hand, it is observed that scanning+pruning often used by the analytical databases can do a good job to handle such these scenarios.

The basic design idea of CeresDB is to adopt a hybrid storage format and the corresponding query method for a better performance in processing both timeseries and analytic workloads.

## Architecture
```plaintext
┌──────────────────────────────────────────┐
│       RPC Layer (HTTP/gRPC/MySQL)        │
└──────────────────────────────────────────┘
┌──────────────────────────────────────────┐
│                 SQL Layer                │
│ ┌─────────────────┐  ┌─────────────────┐ │
│ │     Parser      │  │     Planner     │ │
│ └─────────────────┘  └─────────────────┘ │
└──────────────────────────────────────────┘
┌───────────────────┐  ┌───────────────────┐
│    Interpreter    │  │      Catalog      │
└───────────────────┘  └───────────────────┘
┌──────────────────────────────────────────┐
│               Query Engine               │
│ ┌─────────────────┐  ┌─────────────────┐ │
│ │    Optimizer    │  │    Executor     │ │
│ └─────────────────┘  └─────────────────┘ │
└──────────────────────────────────────────┘
┌──────────────────────────────────────────┐
│         Pluggable Table Engine           │
│  ┌────────────────────────────────────┐  │
│  │              Analytic              │  │
│  │┌────────────────┐┌────────────────┐│  │
│  ││      Wal       ││    Memtable    ││  │
│  │└────────────────┘└────────────────┘│  │
│  │┌────────────────┐┌────────────────┐│  │
│  ││     Flush      ││   Compaction   ││  │
│  │└────────────────┘└────────────────┘│  │
│  │┌────────────────┐┌────────────────┐│  │
│  ││    Manifest    ││  Object Store  ││  │
│  │└────────────────┘└────────────────┘│  │
│  └────────────────────────────────────┘  │
│  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│           Another Table Engine        │  │
│  └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
└──────────────────────────────────────────┘
```

The figure above shows the architecture of CeresDB stand-alone service and the details of some important modules will be described in the following part.

### RPC Layer
module path: https://github.com/CeresDB/ceresdb/tree/main/server

The current RPC supports multiple protocols including HTTP, gRPC, MySQL.

Basically, HTTP and MySQL are used to debug CeresDB, query manually and perform DDL operations (such as creating, deleting tables, etc.). And gRPC protocol can be regarded as a customized protocol for high-performance, which is suitable for massive reading and writing operations.

### SQL Layer
module path: https://github.com/CeresDB/ceresdb/tree/main/sql

SQL layer takes responsibilities for parsing sql and generating the plan.

Based on [sqlparser](https://github.com/sqlparser-rs/sqlparser-rs) a sql dialect, which introduces some key concepts including `Tag` and `Timestamp`, is provided for processing timeseries data. And by utilizing [DataFusion](https://github.com/apache/arrow-datafusion) the planner can generate not only normal logical plans but also custom ones, such as plans for `PromQL`.

### Interpreter
module path: https://github.com/CeresDB/ceresdb/tree/main/interpreters

The `Interpreter` module encapsulates the SQL `CRUD` operations. Actually, a sql received by CeresDB will be parsed, converted into the query plan and then executed in some specific interpreter, such as `SelectInterpreter`, `InsertInterpreter` and etc.

### Catalog
module path: https://github.com/CeresDB/ceresdb/tree/main/catalog_impls

`Catalog` is actually the module managing metadata and the levels of metadata adopted by CeresDB is similar to PostgreSQL: `Catalog > Schema > Table`, but they are only used as namespace.

At present, `Catalog` and `Schema` have two different kinds of implementation for stand-alone and distributed mode because some strategies to generate ids and ways to persist metadata differ in different mode.

### Query Engine
module path: https://github.com/CeresDB/ceresdb/tree/main/query_engine

`Query Engine` is responsible for optimizing and executing query plan given a basic SQL plan provided by SQL layer and now such work is mainly delegated to [DataFusion](https://github.com/apache/arrow-datafusion).

In addition to the basic functions of SQL, CeresDB also defines some customized query protocols and optimization rules for some specific query plans by utilizing the extensibility provided by [DataFusion](https://github.com/apache/arrow-datafusion). For example, the implementation of `PromQL` is implemented in this way and read it if you are interested.

### Pluggable Table Engine
module path: https://github.com/CeresDB/ceresdb/tree/main/table_engine

`Table Engine` is actually a storage engine for managing tables in CeresDB and the pluggability of `Table Engine` is a core design of CeresDB which matters in achieving our target (process both timeseries and analytic workloads well). CeresDB will have multiple kinds of `Table Engine` for different workloads and the most appropriate one should be chosen as the storage engine according to the workload pattern.

Now the requirements for a `Table Engine` are:
- Manage all the shared resources under the engine:
    - Memory
    - Storage
    - CPU
- Manage metadata of tables such as table schema and table options;
- Provide `Table` instances which provides `read` and `write` methods;
- Take responsibilities for creating, opening, dropping and closing `Table` instance;
- ....

Actually the things that a `Table Engine` needs to process are a little complicated. And now in CeresDB only one `Table Engine` called `Analytic` is provided and does a good job in processing analytical workload, but it is not ready yet to handle the timeseries workload (we plan to enhance it for a better performance by adding some indexes which help handle timeseries workload).

The following part gives a description about details of `Analytic Table Engine`.

#### WAL
module path: https://github.com/CeresDB/ceresdb/tree/main/wal

The model of CeresDB processing data is `WAL` + `MemTable` that the recent written data is written to `WAL` first and then to `MemTable` and after a certain amount of data in `MemTable` is accumulated, the data will be organized in a query-friendly form to persistent devices.

Now two implementations of `WAL` are provided for stand-alone and distributed mode:
- For stand-alone mode, `WAL` is based on `RocksDB` and data is persisted on the local disk.
- For distributed mode, `WAL` is required as a distributed component and to be responsible for reliability of the newly written data, so now we provide an implementation based on [OceanBase](https://github.com/oceanbase/oceanbase) and in our roadmap a more lightweight implementation will be provided.

Besides, `WAL`'s trait definition tells that `WAL` has the concept of `Region` and actually each table is assigned to a `Region` so that the isolation between tables is gained and such an isolation provides convenience for some operations on table's level (such as different `TTL`s for different tables).

#### MemTable
module path: https://github.com/CeresDB/ceresdb/tree/main/analytic_engine/src/memtable

`Memtable` is used to store the newly written data and after a certain amount of data is accumulated, CeresDB organizes the data in `MemTable` into a query-friendly storage format (`SST`) and stores it to the persistent device. `MemTable` is readable before it gets persisted (flushed).

The current implementation of `MemTable` is based on [agatedb's skiplist](https://github.com/tikv/agatedb/blob/8510bff2bfde5b766c3f83cf81c00141967d48a4/skiplist). It allows concurrent reads and writes and can control memory usage based on [Arena](https://github.com/CeresDB/ceresdb/tree/main/components/skiplist).

#### Flush
module path: https://github.com/CeresDB/ceresdb/blob/main/analytic_engine/src/instance/flush_compaction.rs

What `Flush` does is that when the memory usage of `MemTable` reaches the threshold, some `MemTables` are selected for flushing into query-friendly `SST`s saved on persistent device.

During the flushing procedure, the data will be divided by a certain time range (which is configured by table option `Segment Duration`), and no `SST` will span the `Segment Duration`. Actually this is also a common operation in most timeseries databases which organizes data in the time dimension to speed up subsequent time-related operations, such as querying data over a time range and assisting purge data outside the `TTL`.

At present, the control process of `Flush` is a little complicated, so the details will be explained in another document.

#### Compaction
module path: https://github.com/CeresDB/ceresdb/tree/main/analytic_engine/src/compaction

The data of `MemTable` is flushed as `SST`s, but the file size of recently flushed `SST` may be very small. And too small or too many `SST`s lead to the poor query performance. Therefore, `Compaction` is then introduced to rearrange the `SST`s so that the multiple smaller `SST` files can be compacted into a larger `SST` file.

The detailed strategy of `Compaction` will also be described with `Flush` in subsequent documents.

#### Manifest
module path: https://github.com/CeresDB/ceresdb/tree/main/analytic_engine/src/meta

`Manifest` takes responsibilities for managing tables' metadata of `Analytic Engine` including:
- Table schema and table options;
- The sequence number where the newest flush finishes;
- The information of `SST`, such as `SST` path.

Now the `Manifest` is based on `WAL` (this is a different instance from the `WAL` mentioned above for newly written data) and in order to avoid infinite expansion of metadata (actually every `Flush` leads to an update on sst information), `Snapshot` is also introduced to clean up the history of metadata updates.

#### Object Store
module path: https://github.com/CeresDB/ceresdb/tree/main/components/object_store

The `SST` generated by `Flush` needs to be persisted and the abstraction of the persistent storage device is `ObjectStore` including multiple implementations:
- Based on local file system;
- Based on [Alibaba Cloud OSS](https://www.alibabacloud.com/product/object-storage-service).

The distributed architecture of CeresDB separates storage and computing, which requires `Object Store` needs to be a highly available and reliable service independent of CeresDB. Therefore, storage systems like [Amazon S3](https://aws.amazon.com/s3/), [Alibaba Cloud OSS](https://www.alibabacloud.com/product/object-storage-service) is a good choice and in the future implementations on storage systems of some other cloud service providers is planned to provide.

#### SST
module path: https://github.com/CeresDB/ceresdb/tree/main/analytic_engine/src/sst

Both `Flush` and `Compaction` involves `SST` and in the codebase `SST` itself is actually an abstraction that can have multiple specific implementations. The current implementation is based on [Parquet](https://parquet.apache.org/), which is a column-oriented data file format designed for efficient data storage and retrieval.

The format of `SST` is very critical for retrieving data and is also the most important part to perform well in handling both timeseries and analytic workloads. At present, our [Parquet](https://parquet.apache.org/)-based implementation is good at processing analytic workload but is poor at processing timeseries workload. In our roadmap, we will explore more storage formats in order to achieve a good performance in both workloads.

#### Space
module path: https://github.com/CeresDB/ceresdb/blob/main/analytic_engine/src/space.rs

In `Analytic Engine`, there is a concept called `space` and here is an explanation for it to resolve some ambiguities when read source code. Actually `Analytic Engine` does not have the concept of `catalog` and `schema` and only provides two levels of relationship: `space` and `table`. And in the implementation, the `schema id` (which should be unique across all `catalog`s) on the upper layer is actually mapped to `space id`.

The `space` in `Analytic Engine` serves mainly for isolation of resources for different tenants, such as the usage of memory.

## Critical Path
After a brief introduction to some important modules of CeresDB, we will give a description for some critical paths in code, hoping to provide interested developers with a guide for reading the code.

### Query
```plaintext
┌───────┐      ┌───────┐      ┌───────┐
│       │──1──▶│       │──2──▶│       │
│Server │      │  SQL  │      │Catalog│
│       │◀─10──│       │◀─3───│       │
└───────┘      └───────┘      └───────┘
                │    ▲
               4│   9│
                │    │
                ▼    │
┌─────────────────────────────────────┐
│                                     │
│             Interpreter             │
│                                     │
└─────────────────────────────────────┘
                           │    ▲
                          5│   8│
                           │    │
                           ▼    │
                   ┌──────────────────┐
                   │                  │
                   │   Query Engine   │
                   │                  │
                   └──────────────────┘
                           │    ▲
                          6│   7│
                           │    │
                           ▼    │
 ┌─────────────────────────────────────┐
 │                                     │
 │            Table Engine             │
 │                                     │
 └─────────────────────────────────────┘
```
Take `SELECT` SQL as an example. The figure above shows the query procedure and the numbers in it indicates the order of calling between the modules.

Here are the details:
- Server module chooses a proper rpc module (it may be HTTP, gRPC or mysql) to process the requests according the protocol used by the requests;
- Parse SQL in the request by the parser;
- With the parsed sql and the catalog/schema module, [DataFusion](https://github.com/apache/arrow-datafusion) can generate the logical plan;
- With the logical plan, the corresponding `Interpreter` is created and logical plan will be executed by it;
- For the logical plan of normal `Select` SQL, it will be executed through `SelectInterpreter`;
- In the `SelectInterpreter` the specific query logic is executed by the `Query Engine`:
    - Optimize the logical plan;
    - Generate the physical plan;
    - Optimize the physical plan;
    - Execute the physical plan;
- The execution of physical plan involves `Analytic Engine`:
    - Data is obtained by `read` method of `Table` instance provided by `Analytic Engine`;
    - The source of the table data is `SST` and `Memtable`, and the data can be filtered by the pushed down predicates;
    - After retrieving the table data, `Query Engine` will complete the specific computation and generate the final results;
- `SelectInterpreter` gets the results and feeds them to the protocol module;
- After the protocol layer converts the results, the server module responds to the client with them.

### Write
```plaintext
┌───────┐      ┌───────┐      ┌───────┐
│       │──1──▶│       │──2──▶│       │
│Server │      │  SQL  │      │Catalog│
│       │◀─8───│       │◀─3───│       │
└───────┘      └───────┘      └───────┘
                │    ▲
               4│   7│
                │    │
                ▼    │
┌─────────────────────────────────────┐
│                                     │
│             Interpreter             │
│                                     │
└─────────────────────────────────────┘
      │    ▲
      │    │
      │    │
      │    │
      │    │       ┌──────────────────┐
      │    │       │                  │
     5│   6│       │   Query Engine   │
      │    │       │                  │
      │    │       └──────────────────┘
      │    │
      │    │
      │    │
      ▼    │
 ┌─────────────────────────────────────┐
 │                                     │
 │            Table Engine             │
 │                                     │
 └─────────────────────────────────────┘
```
Take `INSERT` SQL as an example. The figure above shows the query procedure and the numbers in it indicates the order of calling between the modules.

Here are the details:
- Server module chooses a proper rpc module (it may be HTTP, gRPC or mysql) to process the requests according the protocol used by the requests;
- Parse SQL in the request by the parser;
- With the parsed sql and the catalog/schema module, [DataFusion](https://github.com/apache/arrow-datafusion) can generate the logical plan;
- With the logical plan, the corresponding `Interpreter` is created and logical plan will be executed by it;
- For the logical plan of normal `INSERT` SQL, it will be executed through `InsertInterpreter`;
- In the `InsertInterpreter`, `write` method of `Table` provided `Analytic Engine` is called:
    - Write the data into `WAL` first;
    - Write the data into `MemTable` then;
- Before writing to `MemTable`, the memory usage will be checked. If the memory usage is too high, the flush process will be triggered:
    - Persist some old MemTables as `SST`s;
    - Delete the corresponding `WAL` entries;
    - Updates the manifest for the new `SST`s and the sequence number of `WAL`;
- Server module responds to the client with the execution result.
