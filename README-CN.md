![CeresDB](docs/logo/CeresDB.png)

![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)
[![CI](https://github.com/CeresDB/ceresdb/actions/workflows/ci.yml/badge.svg)](https://github.com/CeresDB/ceresdb/actions/workflows/ci.yml)
[![OpenIssue](https://img.shields.io/github/issues/CeresDB/ceresdb)](https://github.com/CeresDB/ceresdb/issues)
[![Slack](https://badgen.net/badge/Slack/Join%20CeresDB/0abd59?icon=slack)](https://join.slack.com/t/ceresdbcommunity/shared_invite/zt-1au1ihbdy-5huC9J9s2462yBMIWmerTw)

[English](./README.md)

CeresDB 是一款高性能、分布式、Schema-less 的云原生时序数据库，能够同时处理时序型（time-series）以及分析型（analytics）负载。

## 项目状态
项目目前在快速迭代中，早期版本可能存在数据不兼容问题，因此不推荐生产使用及性能测试。

## RoadMap
项目 [Roadmap](./docs/dev/roadmap-CN.md)。

## 快速开始
### 获取代码
通过 git 克隆代码仓库并进入代码目录：
```bash
git clone git@github.com:CeresDB/ceresdb.git
cd ceresdb
```

### 通过 Docker 运行
确保开发环境安装了 docker，通过仓库中的提供的 Dockerfile 进行镜像的构建：
```shell
docker build -t ceresdb .
```

使用编译好的镜像，启动服务：
```shell
docker run -d -t --name ceresdb -p 5440:5440 -p 8831:8831 ceresdb
```

### 通过源码编译运行
#### 安装依赖
目前为了编译 CeresDB，需要安装相关依赖，以及 Rust 工具链。

#### 开发依赖（Ubuntu20.04）
开发环境这里以 Ubuntu20.04 为例，执行如下的命令，即可安装好所需的依赖：
```shell
apt install git curl gcc g++ libssl-dev pkg-config cmake
```

需要注意的是，项目的编译对 cmake、gcc、g++ 等实际上都是有版本要求的，如果开发环境使用的是较老的 Linux 发行版的话，一般需要手动安装较高版本的这些依赖。

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
./target/release/ceresdb-server --config ./docs/example.toml
```

### 进行数据读写
CeresDB 支持自定义扩展的 SQL 协议，目前可以通过 http 服务以 SQL 语句进行数据的读写、表的创建。
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
    "query": "DROP TABLE demo"
}'
```

## 如何贡献
[如何参与 CeresDB 代码贡献](CONTRIBUTING.md)

[约定式提交](docs/dev/conventional-commit.md)

## 架构及技术文档
相关技术文档在[docs](docs)目录中，补充完善中~

## 致谢
CeresDB 部分设计参考 [influxdb_iox](https://github.com/influxdata/influxdb_iox), 部分代码实现参考 [tikv](https://github.com/tikv/tikv) 以及其他的优秀开源项目，感谢 InfluxDB 团队、TiKV 团队，以及其他优秀的开源项目。

## 开源许可
CeresDB 基于 [Apache License 2.0](./LICENSE) 协议。

## 社区
- [CeresDB 社区角色](ROLES.md)
- [Slack](https://join.slack.com/t/ceresdbcommunity/shared_invite/zt-1au1ihbdy-5huC9J9s2462yBMIWmerTw)
- 钉钉群: CeresDB 开源: 44602802
- 邮箱: ceresdb@service.alipay.com
- [微信群](https://github.com/CeresDB/assets/blob/main/WeChatQRCode.jpg)
