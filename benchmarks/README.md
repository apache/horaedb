// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

# Benchmarks

## Test Data
todo

## Config
A config template can be found in `config/bench.toml`.

## Run benchmarks
In root directory of `ceresdbx` (not this directory `ceresdbx/benchmarks`), run the following command:
```bash
ANALYTIC_BENCH_CONFIG_PATH=/path/to/bench.toml cargo bench -p benchmarks
```

Print logs:
```bash
RUST_LOG=info ANALYTIC_BENCH_CONFIG_PATH=/path/to/bench.toml cargo bench -p benchmarks
```

Run specific bench:
```bash
ANALYTIC_BENCH_CONFIG_PATH=/path/to/bench.toml cargo bench -p benchmarks -- read_parquet
```
