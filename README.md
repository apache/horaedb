![HoraeDB](docs/logo/horaedb-banner-white-small.jpg)

![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)
[![CI](https://github.com/apache/horaedb/actions/workflows/ci.yml/badge.svg)](https://github.com/apache/horaedb/actions/workflows/ci.yml)
[![OpenIssue](https://img.shields.io/github/issues/apache/horaedb)](https://github.com/apache/horaedb/issues)
[![HoraeDB Docker](https://img.shields.io/docker/v/apache/horaedb-server?logo=docker&label=horaedb-server)](https://hub.docker.com/r/apache/horaedb-server)
[![HoraeMeta Docker](https://img.shields.io/docker/v/apache/horaemeta-server?logo=docker&label=horaemeta-server)](https://hub.docker.com/r/apache/horaemeta-server)

[中文](./README-CN.md)

Apache HoraeDB (incubating) is a high-performance, distributed, cloud native time-series database.

> [!IMPORTANT]
> Apache HoraeDB (incubating) is an effort undergoing incubation at the Apache
> Software Foundation (ASF), sponsored by the Apache Incubator PMC.
>
> Please read the [DISCLAIMER](DISCLAIMER) and a full explanation of ["incubating"](https://incubator.apache.org/policy/incubation.html).

## Documentation

- [User Guide](https://horaedb.apache.org/docs/getting-started/)
- [Development Guide](https://horaedb.apache.org/docs/dev/)

## Quick Start

### Run with Docker

#### Run HoraeDB standalone Server

```
docker run -d --name horaedb-server \
  -p 8831:8831 \
  -p 3307:3307 \
  -p 5440:5440 \
  apache/horaedb-server
```

#### Run HoraeDB cluster with two horaedb-server node and one horaemeta-server node.

```
docker compose -f docker/docker-compose.yaml up
```

### Run from source code

Please read the [development guide](https://horaedb.apache.org/docs/dev/compile_run/) guide for instructions on how to build.

### Create Table and Write/Read data
Create Table.

```
curl --location --request POST 'http://127.0.0.1:5440/sql' \
-d '
CREATE TABLE `demo` (
    `name` string TAG,
    `value` double NOT NULL,
    `t` timestamp NOT NULL,
    timestamp KEY (t))
ENGINE=Analytic
  with
(enable_ttl="false")
'
```

Write data with SQL.

```
curl --location --request POST 'http://127.0.0.1:5440/sql' \
-d '
INSERT INTO demo (t, name, value)
    VALUES (1702224000000, "horaedb", 100)
'
```

Read data with SQL.

```
curl --location --request POST 'http://127.0.0.1:5440/sql' \
-d '
SELECT * FROM `demo`
'
```

Drop table.

```
curl --location --request POST 'http://127.0.0.1:5440/sql' \
-d '
Drop TABLE `demo`
'
```


## Community

Thrive together in Apache HoraeDB (incubating) community with users and developers from all around the world.

- Ask questions on [GitHub Discussion](https://github.com/apache/horaedb/discussions).
- Chat with developers/users on [Discord](https://discord.gg/h5r4kVMRYN) or [DingTalk](https://horaedb.apache.org/images/dingtalk.jpg).
- Mailing lists are a form of communication used by the Apache community. See guide [here](http://horaedb.apache.org/community/) to subscribe to our list.

> Read our [Contributing Guide](CONTRIBUTING.md) and make your first contribution!

## Acknowledgment

When develop we benefit a lot from several other open source projects, such as [influxdb_iox](https://github.com/influxdata/influxdb/tree/main), [tikv](https://github.com/tikv/tikv) etc, thanks for their awesome work.

## License

[Apache License 2.0](./LICENSE)
