[CeresDB](https://github.com/CeresDB/ceresdb) is a high-performance, distributed, schema-less, cloud native time-series database that can handle both time-series and analytics workloads.

# How to use this image

You can use command below to start a standalone server
```bash
docker run --name ceresdb-server \
  -p 8831:8831 \
  -p 3307:3307 \
  -p 5440:5440 \
  ceresdb/ceresdb-server:v0.2.0
```

CeresDB will listen three ports when start:
- 8831, gRPC port
- 3307, MySQL port
- 5440, HTTP port

There are some files used by server inside Docker image, the following two are most important:
- `/home/admin/logs/ceresdb/out.log`, log
- `/etc/ceresdb/ceresdb.toml`, config


# Write and read data
- [Quick Start](https://github.com/CeresDB/ceresdb/blob/main/docs/user-guide/src/quick_start.md)
