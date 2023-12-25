![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)
[![CI](https://github.com/apache/incubator-horaedb/actions/workflows/ci.yml/badge.svg)](https://github.com/apache/incubator-horaedb/actions/workflows/ci.yml)
[![OpenIssue](https://img.shields.io/github/issues/apache/incubator-horaedb)](https://github.com/apache/incubator-horaedb/issues)
<!-- [![Docker](https://img.shields.io/docker/v/horaedb/horaedb-server?logo=docker)](https://hub.docker.com/r/horaedb/horaedb-server) TODO need to wait for first apache version release.-->

[中文](./README-CN.md)

HoraeDB is a high-performance, distributed, cloud native time-series database.

## Documentation

- [User Guide](https://apache.github.io/incubator-horaedb-docs)
- [Development Guide](https://apache.github.io/incubator-horaedb-docs/dev/compile_run.html)
- [Roadmap](https://apache.github.io/incubator-horaedb-docs/dev/roadmap.html)

## Quick Start

### Run with Docker

Run HoraeDB standalone Server.

```
docker run -d --name horaedb-server \
  -p 8831:8831 \
  -p 3307:3307 \
  -p 5440:5440 \
  ghcr.io/apache/horaedb-server:nightly-20231222-f57b3827
```

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

### Run from source code

See details [here](https://apache.github.io/incubator-horaedb-docs/dev/compile_run.html).

## Contributing

Any contribution is welcome!

Discuss at [dev mailing list](mailto:dev-subscribe@horaedb.apache.org) ([subscribe](mailto:dev-subscribe@horaedb.apache.org?subject=(send%20this%20email%20to%20subscribe)) / [unsubscribe](mailto:dev-unsubscribe@horaedb.apache.org?subject=(send%20this%20email%20to%20unsubscribe)) / [archives](https://lists.apache.org/list.html?dev@horaedb.apache.org))

Read our [Contributing Guide](CONTRIBUTING.md) and make your first contribution!

## Acknowledgment

When develop HoraeDB, we benefit a lot from several other open source projects,  such as [influxdb_iox](https://github.com/influxdata/influxdb/tree/main/influxdb_iox), [tikv](https://github.com/tikv/tikv) etc, thanks for their awesome work.

In our production usage, we heavily use [OceanBase](https://github.com/oceanbase/oceanbase) as implementation of both WAL and ObjectStorage, and OceanBase team help us maintain stability of our cluster, thanks for their kindly support.

## License

HoraeDB is under [Apache License 2.0](./LICENSE).
