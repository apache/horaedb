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
- [ ] Formally release CeresDB and its SDKs. All breaking changes will be finished.
- [ ] Finish the majority of work related to PartitionTable.
- [ ] Various efforts to improve query performance, especially for cloud-native cluster mode. Theses works includes:
    - Multi-tier cacheing.
    - Introduce various methods to reduce the data fetched from remote storage (improve the accuracy of SST data filtering).
    - Increase the parallelism while fetching data from remote object-store.
    - Add some resource-controlling methods.

### Afterwards
With an in-depth understanding of the time-series database and its various use cases, the majority of our work will be focused on performance, scalability, ease of use, and collaborations with open-source communities.
- [ ] Add utilities that support promQL, influxQL, opentsdb protocol, and so on.
- [ ] Provide basic operation and maintenance tools which fit well for deployments on Kubernetes or bare servers.
- [ ] Support more underlying storages for adaptivity. For example, the underlying storage could be a distributed file system.
- [ ] Develop various tools that ease the use of CeresDB. For example, data import and export tools.
