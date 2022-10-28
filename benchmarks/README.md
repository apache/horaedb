# Benchmarks

## Test Data
todo

## Config
A config template can be found in `config/bench.toml`.

## Run benchmarks
In root directory of `ceresdb` (not this directory `ceresdb/benchmarks`), run the following command:
```bash
ANALYTIC_BENCH_CONFIG_PATH=/path/to/bench.toml cargo bench -p benchmarks
```

Print logs:
```bash
RUST_LOG=info ANALYTIC_BENCH_CONFIG_PATH=/path/to/bench.toml cargo bench -p benchmarks
```

Run specific bench:
```bash
ANALYTIC_BENCH_CONFIG_PATH=/path/to/bench.toml cargo bench --bench bench -p benchmarks -- read_parquet
```

If you want to enable pprof, add `--profile-time 60`, see [pprof-rs#127](https://github.com/tikv/pprof-rs/issues/127)
