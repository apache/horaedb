![logo](https://github.com/CeresDB/ceresdb/raw/main/docs/logo/CeresDB.png)


[CeresDB](https://github.com/CeresDB/ceresdb) is a high-performance, distributed, schema-less, cloud native time-series database that can handle both time-series and analytics workloads.

# How to use this image

You can use command below to start a standalone server in background:
```bash
docker run -d --name ceresdb-server \
  -p 8831:8831 \
  -p 3307:3307 \
  -p 5440:5440 \
  ceresdb/ceresdb-server:v0.2.0
```

CeresDB will listen three ports when start:
- 8831, gRPC port
- 3307, MySQL port
- 5440, HTTP port

There are some files used by server inside Docker image, the following one is the most important:
- `/etc/ceresdb/ceresdb.toml`, config

You overwrite config with this command:

```bash
docker run -d --name ceresdb-server \
  -p 8831:8831 \
  -p 3307:3307 \
  -p 5440:5440 \
  -v $(path/to/config):/etc/ceresdb/ceresdb.toml \
  ceresdb/ceresdb-server:$(version)
```

# Documentation
- https://docs.ceresdb.io/
