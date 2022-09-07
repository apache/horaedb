# Ceresmeta

![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)

CeresMeta is the meta service for managing the CeresDB cluster.

## Status
The project is in a very early stage.

## Quick Start
### Build ceresmeta binary
```bash
make build
```

### Standalone Mode
Although CeresMeta is designed to deployed as a cluster with three or more instances, it can also be started standalone:
```bash
# Set correct HostIP here.
export HostIP0={your_ip}

# ceresmeta0
mkdir /tmp/ceresmeta0
./ceresmeta -etcd-start-timeout-ms 30000 \
            -peer-urls "http://${HostIP0}:2380" \
            -advertise-client-urls "http://${HostIP0}:2379" \
            -advertise-peer-urls "http://${HostIP0}:2380" \
            -client-urls "http://${HostIP0}:2379" \
            -wal-dir /tmp/ceresmeta0/wal \
            -data-dir /tmp/ceresmeta0/data \
            -node-name "meta0" \
            -etcd-log-file /tmp/ceresmeta0/etcd.log \
            -initial-cluster "meta0=http://${HostIP0}:2380"
```

### Cluster mode
Here is an example for starting CeresMeta in cluster mode (three instances) on single machine by using different ports:
```bash
# Set correct HostIP here.
export HostIP0={your_ip}
export HostIP1={your_ip}
export HostIP2={your_ip}

# Create directories.
mkdir /tmp/ceresmeta0
mkdir /tmp/ceresmeta1
mkdir /tmp/ceresmeta2

# Ceresmeta0
./ceresmeta -etcd-start-timeout-ms 30000 \
            -peer-urls "http://${HostIP0}:2380" \
            -advertise-client-urls "http://${HostIP0}:2379" \
            -advertise-peer-urls "http://${HostIP0}:2380" \
            -client-urls "http://${HostIP0}:2379" \
            -wal-dir /tmp/ceresmeta0/wal \
            -data-dir /tmp/ceresmeta0/data \
            -node-name "meta0" \
            -etcd-log-file /tmp/ceresmeta0/etcd.log \
            -initial-cluster "meta0=http://${HostIP0}:2380,meta1=http://${HostIP1}:12380,meta2=http://${HostIP2}:22380"

# Ceresmeta1
./ceresmeta -etcd-start-timeout-ms 30000 \
            -peer-urls "http://${HostIP1}:12380" \
            -advertise-client-urls "http://${HostIP1}:12379" \
            -advertise-peer-urls "http://${HostIP1}:12380" \
            -client-urls "http://${HostIP1}:12379" \
            -wal-dir /tmp/ceresmeta1/wal \
            -data-dir /tmp/ceresmeta1/data \
            -node-name "meta1" \
            -etcd-log-file /tmp/ceresmeta1/etcd.log \
            -initial-cluster "meta0=http://${HostIP0}:2380,meta1=http://${HostIP1}:12380,meta2=http://${HostIP2}:22380"

# Ceresmeta2
./ceresmeta -etcd-start-timeout-ms 30000 \
            -peer-urls "http://${HostIP2}:22380" \
            -advertise-client-urls "http://${HostIP2}:22379" \
            -advertise-peer-urls "http://${HostIP2}:22380" \
            -client-urls "http://${HostIP2}:22379" \
            -wal-dir /tmp/ceresmeta2/wal \
            -data-dir /tmp/ceresmeta2/data \
            -node-name "meta2" \
            -etcd-log-file /tmp/ceresmeta2/etcd.log \
            -initial-cluster "meta0=http://${HostIP0}:2380,meta1=http://${HostIP1}:12380,meta2=http://${HostIP2}:22380"
```

## Acknowledgment
CeresMeta refers to the excellent project [pd](https://github.com/tikv/pd) in design and some module and codes are forked from [pd](https://github.com/tikv/pd), thanks to the TiKV team.

## Contributing
The project is under rapid development so that any contribution is welcome.
Check our [Contributing Guide](https://github.com/CeresDB/ceresmeta/blob/main/CONTRIBUTING.md) and make your first contribution!

## License
CeresMeta is under [Apache License 2.0](./LICENSE).
