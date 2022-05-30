# CeresDB 

![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)

CeresDB 诞生于蚂蚁集团内部，是一个分布式、高可用、高可靠的时间序列数据库 [Time Series Database](https://en.wikipedia.org/wiki/Time_series_database?spm=ata.21736010.0.0.7d707536F4fwj8)。
经过多年双11打磨，作为蚂蚁全站监控数据存储的时间序列数据库，承载了每天数万亿数据点的写入，并提供多维度查询。

## 目标特性
- 兼容 Opentsdb、Prometheus 查询协议
- 支持 SQL 查询分析
- 计算存储分离 
- 多种存储引擎，时序分析引擎
- 使用 [Apache Arrow](https://arrow.apache.org/)
- 分布式、动态扩缩容
- 高可用
- 高压缩比

## 需要
编译需要 rust nightly 版本, 最小版本 nightly-2022-01-06

## 快速开始
### 获取代码
直接通过 git 进行下载：
```bash
git clone git@github.com:CeresDB/ceresdb.git
```

进入代码目录：
```bash
cd ceresdb
```

### 通过镜像运行
确保开发环境安装了 docker，通过仓库中的提供的 Dockerfile 进行镜像的构建：
```shell
docker build -t ceresdb:0.1.0 .
```

使用编译好的镜像，启动服务：
```shell
docker run -d -t --name ceresdb -p 5440:5440 -p 8831:8831 ceresdb:0.1.0
```

### 通过源码编译运行
#### 安装依赖
目前为了编译 CeresDB，需要安装相关依赖，以及 Rust 工具链。

#### 开发依赖（Ubuntu20.04）
开发环境这里以 Ubuntu20.04 为例，执行如下的命令，即可安装好所需的依赖：
```shelll
apt install git curl gcc g++ libssl-dev pkg-config cmake
```

需要注意的是，项目的编译对 cmake、gcc、g++ 等实际上都是有版本要求的，如果是在比较老的 Linux 发行版上面的话，一般需要手动安装较高版本的这些依赖。

#### Rust
Rust 的安装建议通过 [rustup](https://rustup.rs/)，安装了 rustup 之后，进入到 CeresDB 项目的时候，会自动根据 rust-toolchain 文件下载指定的 Rust 版本。

理论上执行了之后，需要添加环境变量，才能使用 Rust 工具链，一般会把下面的命令放入到自己的 `~/.bashrc` 或者 `~/.bash_profile` 中：
```bash
source $HOME/.cargo/env
```

#### 编译和运行
编译 Release 版本，执行如下命令：
```
cargo build --release
```

使用下载的代码中提供的默认配置文件，即可启动：
```bash
./target/ceresdb-server --config ./docs/example.toml
```

### 进行数据读写
CeresDB 支持自定义扩展的 SQL 协议，目前可以通过 http 服务以 SQL 语句进行数据的读写、表的创建。
#### 建表
```shell
curl --location --request POST 'http://127.0.0.1:5440/sql' \
--header 'Content-Type: application/json' \
--data-raw '{
    "query": "CREATE TABLE `demo` (`name` string TAG, `value` double NOT NULL, `t` timestamp NOT NULL, TIMESTAMP KEY(t)) ENGINE=Analytic with (enable_ttl='\''false'\'');"
}'
```

#### 插入数据
```shell
curl --location --request POST 'http://127.0.0.1:5440/sql' \
--header 'Content-Type: application/json' \
--data-raw '{
    "query": "INSERT INTO demo(t, name, value) VALUES(1651737067000, '\''ceresdb'\'', 100)"
}'
```

#### 查询数据
```shell
curl --location --request POST 'http://127.0.0.1:5440/sql' \
--header 'Content-Type: application/json' \
--data-raw '{
    "query": "select * from demo"
}'
```

#### 查看建表信息
```shell
curl --location --request POST 'http://127.0.0.1:5440/sql' \
--header 'Content-Type: application/json' \
--data-raw '{
    "query": "show create table demo"
}'
```

#### 删除表
```shell
curl --location --request POST 'http://127.0.0.1:5440/sql' \
--header 'Content-Type: application/json' \
--data-raw '{
    "query": "DROP TABLE demo;"
}'
```

## 如何贡献
[如何参与 CeresDB 代码贡献](./docs/conventional-commit.md)

## 致谢
CeresDB 部分设计参考 [influxdb_iox](https://github.com/influxdata/influxdb_iox) 、[tikv](https://github.com/tikv/tikv)，感谢 influxdb、pingcap 相关团队。

## 开源许可
CeresDB 基于 [Apache License 2.0](./LICENSE) 协议，CeresDB 依赖了一些第三方组件，它们的开源协议也为 Apache License 2.0，
另外 CeresDB 也直接引用了一些开源协议为 Apache License 2.0 的代码（可能有一些小小的改动）包括：
- [Agatedb](https://github.com/tikv/agatedb) 中的 skiplist 

## 社区
- [CeresDB 社区角色](./ROLES.md).

- 钉钉群: CeresDB 开源: 44602802
- 微信: chunshao2008
- 邮箱: ceresdb@service.alipay.com
