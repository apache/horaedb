## RoadMap
### v0.1.0
- [x] Standalone version, local storage
- [x] Analytical storage format
- [x] Support SQL

### v0.2.0
- [x] Distributed version supports static topology defined in config file.
- [x] The underlying storage supports Aliyun OSS.
- [x] WAL implementation based on [OBKV](https://github.com/oceanbase/oceanbase).

### v0.3.0
- [x] Release multi-language clients, including Java, Rust and Python.
- [x] Static cluster mode with `CeresMeta`.
- [x] Basic implementation of hybrid storage format.

### v0.4.0
- [x] Implement more sophisticated cluster solution that enhances reliability and scalability of CeresDB.
- [x] Set up nightly benchmark with TSBS.

### v1.0.0-alpha (Released)
- [x] Implement Distributed WAL based on `Apache Kafka`.
- [x] Release Golang client.
- [x] Improve the query performance for traditional time series workloads.
- [x] Support dynamic migration of tables in cluster mode.

### v1.0.0
- [ ] Formally release CeresDB and its SDKs with all breaking changes finished.
- [ ] Finish the majority of work related to `Table Partitioning`.
- [ ] Various efforts to improve query performance, especially for cloud-native cluster mode. These works includes:
    - Multi-tier cache.
    - Introduce various methods to reduce the data fetched from remote storage (improve the accuracy of SST data filtering).
    - Increase the parallelism while fetching data from remote object-store.
- [ ] Improve data ingestion performance by introducing resource control over compaction.

### Afterwards
With an in-depth understanding of the time-series database and its various use cases, the majority of our work will focus on performance, reliability, scalability, ease of use, and collaborations with open-source communities.
- [ ] Add utilities that support `PromQL`, `InfluxQL`, `OpenTSDB` protocol, and so on.
- [ ] Provide basic utilities for operation and maintenance. Specifically, the following are included:
    - Deployment tools that fit well for cloud infrastructures like `Kubernetes`.
    - Enhance self-observability, especially critical logs and metrics should be supplemented.
- [ ] Develop various tools that ease the use of CeresDB. For example, data import and export tools.
- [ ] Explore new storage formats that will improve performance on hybrid workloads (analytical and traditional time-series workloads).
