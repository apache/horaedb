# HoraeMeta

[![codecov](https://codecov.io/gh/apache/incubator-horaedb-meta/branch/main/graph/badge.svg?token=VTYXEAB2WU)](https://codecov.io/gh/apache/incubator-horaedb-meta)
![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)

HoraeMeta is the meta service for managing the HoraeDB cluster.

## Status
The project is in a very early stage.

## Quick Start
### Build HoraeMeta binary
```bash
make build
```

### Standalone Mode
Although HoraeMeta is designed to deployed as a cluster with three or more instances, it can also be started standalone:
```bash
# HoraeMeta0
mkdir /tmp/meta0
./bin/horaemeta-server --config ./config/example-standalone.toml
```

### Cluster mode
Here is an example for starting HoraeMeta in cluster mode (three instances) on single machine by using different ports:
```bash
# Create directories.
mkdir /tmp/meta0
mkdir /tmp/meta1
mkdir /tmp/meta2

# horaemeta0
./bin/horaemeta-server --config ./config/exampl-cluster0.toml

# horaemeta1
./bin/horaemeta-server --config ./config/exampl-cluster1.toml

# horaemeta2
./bin/horaemeta-server --config ./config/exampl-cluster2.toml
```

## Acknowledgment
HoraeMeta refers to the excellent project [pd](https://github.com/tikv/pd) in design and some module and codes are forked from [pd](https://github.com/tikv/pd), thanks to the TiKV team.

## Contributing
The project is under rapid development so that any contribution is welcome.
Check our [Contributing Guide](https://github.com/apache/incubator-horaedb-meta/blob/main/CONTRIBUTING.md) and make your first contribution!

## License
HoraeMeta is under [Apache License 2.0](./LICENSE).
