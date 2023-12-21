![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)
[![CI](https://github.com/apache/incubator-horaedb/actions/workflows/ci.yml/badge.svg)](https://github.com/apache/incubator-horaedb/actions/workflows/ci.yml)
[![OpenIssue](https://img.shields.io/github/issues/apache/horaedb)](https://github.com/apache/horaedb/issues)
<!-- [![Docker](https://img.shields.io/docker/v/apache/horaedb-server?logo=docker)](https://hub.docker.com/r/apache/horaedb-server) TODO need to wait for first apache version release. --> 

[English](./README.md)

HoraeDB 是一款高性能、分布式的云原生时序数据库。

## RoadMap
项目 [Roadmap](https://apache.github.io/incubator-horaedb-docs)。

## 快速开始
### 获取代码
通过 git 克隆代码仓库并进入代码目录：
```bash
git clone git@github.com:apache/incubator-horaedb.git horaedb
cd horaedb
```
<!-- TODO need to wait for first apache version release. -->
<!-- ### 通过 Docker 运行
确保开发环境安装了 docker，通过仓库中的提供的 Dockerfile 进行镜像的构建：
```shell
docker build -t horaedb .
```

使用编译好的镜像，启动服务：
```shell
docker run -d -t --name horaedb -p 5440:5440 -p 8831:8831 horaedb
``` -->

### 通过源码编译运行
#### 安装依赖
目前为了编译 HoraeDB，需要安装相关依赖，以及 Rust 工具链。

#### 开发依赖（Ubuntu20.04）
开发环境这里以 Ubuntu20.04 为例，执行如下的命令，即可安装好所需的依赖：
```shell
sudo apt install git curl gcc g++ libssl-dev pkg-config cmake
```

需要注意的是，项目的编译对 cmake、gcc、g++ 等实际上都是有版本要求的，如果开发环境使用的是较老的 Linux 发行版的话，一般需要手动安装较高版本的这些依赖。


#### 开发依赖（MacOS）
开发环境这里以 MacOS Monterey 为例，执行如下的命令，即可安装好所需的依赖。

1. 安装命令行工具:
```shell
xcode-select --install
```
2. 通过Brew安装cmake:
```shell
brew install cmake
```

#### Rust
Rust 的安装建议通过 [rustup](https://rustup.rs/)，安装了 rustup 之后，进入到 HoraeDB 项目的时候，会自动根据 rust-toolchain 文件下载指定的 Rust 版本。

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
./target/release/horaedb-server --config ./docs/minimal.toml
```

### 进行数据读写
HoraeDB 支持自定义扩展的 SQL 协议，目前可以通过 http 服务以 SQL 语句进行数据的读写、表的创建。
#### 建表
```shell
curl --location --request POST 'http://127.0.0.1:5440/sql' \
--header 'Content-Type: application/json' \
--data-raw '{
    "query": "CREATE TABLE `demo` (`name` string TAG, `value` double NOT NULL, `t` timestamp NOT NULL, TIMESTAMP KEY(t)) ENGINE=Analytic with (enable_ttl='\''false'\'')"
}'
```

#### 插入数据
```shell
curl --location --request POST 'http://127.0.0.1:5440/sql' \
--header 'Content-Type: application/json' \
--data-raw '{
    "query": "INSERT INTO demo(t, name, value) VALUES(1651737067000, '\''horaedb'\'', 100)"
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
    "query": "DROP TABLE demo"
}'
```

### 支持使用 MySQL Client 连接

HoraeDB 也支持使用 MySQL Client 连接执行 SQL

```shell
mysql -u root -P 3307 -h 127.0.0.1
```

## 平台支持

|          target          |         OS        |         status        |
|:------------------------:|:-----------------:|:---------------------:|
| x86_64-unknown-linux-gnu |    kernel 4.9+    |       构建及运行        |
|    x86_64-apple-darwin   | 10.15+, Catalina+ |          构建          |
|    aarch64-apple-darwin  |   11+, Big Sur+   |          构建          |
| aarch64-unknown-linux-gnu|        TBD        | 详见 [#63](https://github.com/apache/incubator-horaedb/issues/63)|
|         windows          |         *         |         未支持         |

## 如何贡献
[如何参与 HoraeDB 代码贡献](CONTRIBUTING.md)

[约定式提交](https://apache.github.io/incubator-horaedb-docs/cn/dev/conventional_commit)

## 架构及技术文档
相关技术文档请参考[docs](https://apache.github.io/incubator-horaedb-docs/)。

## 致谢
在开发 HoraeDB 的过程中， 我们受到很多开源项目的影响和启发，例如  [influxdb_iox](https://github.com/influxdata/influxdb/tree/main/influxdb_iox), [tikv](https://github.com/tikv/tikv) 等等。感谢这些杰出的项目。

在生产环境中，我们重度使用 [OceanBase](https://github.com/oceanbase/oceanbase) 作为 WAL 和 ObjectStore 的实现，而且 OceanBase 团队还帮助我们一起维护集群的稳定，感谢 OceanBase 团队一直以来的帮助。

## 开源许可
HoraeDB 基于 [Apache License 2.0](LICENSE) 协议。
