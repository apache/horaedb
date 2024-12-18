# Benchmarks

A config template can be found in [here](config.toml).

## How to run
```bash
ANALYTIC_BENCH_CONFIG_PATH=/path/to/config cargo bench -p benchmarks
```

Set `RUST_LOG=debug` to enable verbose log.

## Run specific bench:
```bash
ANALYTIC_BENCH_CONFIG_PATH=/path/to/config cargo bench --bench bench -p benchmarks -- bench_encoding
```
