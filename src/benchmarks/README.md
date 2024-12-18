# Benchmarks

## Test Data
todo

## Config
A config template can be found in `config/bench.toml`.

## Run benchmarks
```bash
ANALYTIC_BENCH_CONFIG_PATH=/path/to/bench.toml cargo bench -p benchmarks
```

Print logs:
```bash
RUST_LOG=info ANALYTIC_BENCH_CONFIG_PATH=/path/to/bench.toml cargo bench -p benchmarks
```

Run specific bench:
```bash
ANALYTIC_BENCH_CONFIG_PATH=/path/to/bench.toml cargo bench --bench bench -p benchmarks -- bench_encoding
```
