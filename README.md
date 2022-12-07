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
./ceresmeta --config ./config/example-standalone.toml
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
./ceresmeta --config ./config/exampl-cluster0.toml

# Ceresmeta1
./ceresmeta --config ./config/exampl-cluster1.toml

# Ceresmeta2
./ceresmeta --config ./config/exampl-cluster2.toml
```

## Acknowledgment
CeresMeta refers to the excellent project [pd](https://github.com/tikv/pd) in design and some module and codes are forked from [pd](https://github.com/tikv/pd), thanks to the TiKV team.

## Contributing
The project is under rapid development so that any contribution is welcome.
Check our [Contributing Guide](https://github.com/CeresDB/ceresmeta/blob/main/CONTRIBUTING.md) and make your first contribution!

## License
CeresMeta is under [Apache License 2.0](./LICENSE).
