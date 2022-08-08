## RoadMap
### v0.1.0
- [x] 单机版本，本地存储
- [x] 分析型存储格式
- [x] 支持 SQL 查询、写入

### v0.2.0（已发布）
- [x] 使用配置文件定义静态拓扑的分布式版本
- [x] 底层存储支持阿里云 OSS
- [x] 基于 [OBKV](https://github.com/oceanbase/oceanbase) 的 WAL 实现

### v0.3.0
- [ ] 混合存储格式
- [ ] 基于`CeresMeta`的静态集群模式
- [ ] 多语言客户端，包括 Java, Rust 和 Python

### 后续特性
- [ ] 支持 Prometheus 协议
- [ ] 分布式 WAL
- [ ] 支持动态扩容的集群模式