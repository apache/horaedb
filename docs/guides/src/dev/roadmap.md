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

### v0.4.0 (Released)
- [x] Implement more sophisticated cluster solution that enhances reliability and scalability of CeresDB.
- [x] Set up nightly benchmark with TSBS.

### v1.0.0-alpha
- [ ] Implement Distributed WAL based on `Apache Kafka`.
- [ ] Release Golang client.
- [ ] Improve the query performance for traditional time series workloads.

### Afterward
- [ ] Support Prometheus protocol.
- [ ] Implement UDF framework which makes CeresDB more extensible.
- [ ] Support more underlying storages for adaptivity. For example, the underlying storage could be a distributed file system.
- [ ] Various tools that ease the use of CeresDB. For example, data import and export tools.
