

[HoraeDB](https://github.com/apache/incubator-horaedb) is a high-performance, distributed, schema-less, cloud native time-series database that can handle both time-series and analytics workloads.

# How to use this image

You can use command below to start a standalone server in background:
```bash
docker run -d --name horaedb-server \
  -p 8831:8831 \
  -p 3307:3307 \
  -p 5440:5440 \
  horaedb/horaedb-server:$(version)
```

HoraeDB will listen three ports when started:

- 8831, gRPC port
- 3307, MySQL port
- 5440, HTTP port

There are some files used by server inside Docker image, the following one is the most important:
- `/etc/horaedb/horaedb.toml`, config

You overwrite config with this command:

```bash
docker run -d --name horaedb-server \
  -p 8831:8831 \
  -p 3307:3307 \
  -p 5440:5440 \
  -v $(path/to/config):/etc/horaedb/horaedb.toml \
  horaedb/horaedb-server:$(version)
```

# Documentation

- https://apache.github.io/incubator-horaedb-docs/
