# Remote Write Parser

A hand-written [Prometheus Remote Write Request (V1)](https://prometheus.io/docs/specs/prw/remote_write_spec/) parser optimized for zero-allocation.

## Implementation

Key optimization techniques:

- Object pooling backed by deadpool.

- `RepeatedField` data structures.

- Zero-copy bytes split backed by unsafe magic.

- Manual loop unrolling and function inline optimization.

## Usage

This crate parses the protobuf `string` type as Rust `Bytes` instances instead of `String` instances to avoid allocation, and it **does not** perform UTF-8 validation when parsing. Therefore, it is up to the caller to decide how to make use of the parsed `Bytes` and whether to apply UTF-8 validation.

### Basic Usage

Synchronous parse:

```rust
use bytes::Bytes;
use remote_write::pooled_parser::PooledParser;

let parser = PooledParser::default();
let request = parser.decode(data)?; // Return a `PooledWriteRequest` instance.
```

Asynchronous parse:

```rust
use bytes::Bytes;
use remote_write::pooled_parser::PooledParser;

let parser = PooledParser::default();
let request = parser.decode_async(data).await?; // Return a deadpool object wrapper.

// Access the parsed data through the deadpool object wrapper.
println!("Parsed {} timeseries", request.timeseries.len());
for ts in &request.timeseries {
    for label in &ts.labels {
        let name = String::from_utf8_lossy(&label.name);
        let value = String::from_utf8_lossy(&label.value);
        println!("Label: {}={}", name, value);
    }
}
// Object automatically returned to the pool when dropped.
```

Note: if you use `decode_async()`, you need to add `deadpool` to your dependencies:

```toml
[dependencies]
deadpool = { workspace = true }  # or "0.10" if not inside this workspace.
```

### Enable Unsafe Optimization

```toml
[dependencies]
remote_write = { path = ".", features = ["unsafe-split"] }
```

This feature enables zero-copy bytes split that bypasses Rust's memory safety guarantees. The parsed `Bytes` instances **cannot outlive** the input raw protobuf `Bytes` instance.

Correct usage example:

```rust
use bytes::Bytes;
use remote_write::pooled_parser::PooledParser;

// CORRECT: consume immediately.
async fn foo() -> Result<Vec<String>, Box<dyn Error>> {
    let raw_data = load_protobuf_data(); // Bytes.
    let parser = PooledParser::default();
    let request = parser.decode_async(raw_data).await?;

    let metric_names: Vec<String> = request
        .timeseries
        .iter()
        .flat_map(|ts| ts.labels.iter())
        .filter(|label| label.name.as_ref() == b"__name__")
        .filter_map(|label| String::from_utf8(label.value.to_vec()).ok())
        .collect();

    Ok(metric_names)
}
```

Wrong usage example:

```rust
// WRONG: returned parsed data outlives the input.
fn bar() -> PooledWriteRequest {
    let raw_data = load_protobuf_data(); // Bytes
    let parser = PooledParser::default();
    parser.decode(raw_data).unwrap()
    // The input `raw_data` is dropped here, but the returned `PooledWriteRequest` outlives it.
}
```

## Performance

### CPU Time

Change to target directory by running:

```shell
cd src/benchmarks
```

And run:

```shell
BENCH_CONFIG_PATH=config.toml cargo bench --bench bench remote_write
```

You can also enable the unsafe optimization by running:

```shell
BENCH_CONFIG_PATH=config.toml cargo bench --features unsafe-split --bench bench remote_write
```

### Memory Allocation

Install requirements by runnning:

```shell
pip3 install tabulate
```

Change to target directory by running:

```shell
cd src/benchmarks
```

Run the script:

```shell
python3 remote_write_memory_bench.py --mode sequential --scale 10
```

You can also enable the unsafe optimization by running:

```shell
python3 remote_write_memory_bench.py --mode concurrent --scale 10 --unsafe
```

### Object Pool Efficiency

Change to target directory by running:

```shell
cd src/benchmarks
```

And run:

```shell
cargo run --bin pool_stats --release
```

## Acknowledgements

- The two test data files in `src/remote_write/tests/workloads` are taken from [prom-write-request-bench](https://github.com/v0y4g3r/prom-write-request-bench/tree/main/assets).

- The `src/pb_types/protos/remote_write.proto` file is modified from the official [remote.proto](https://github.com/prometheus/prometheus/blob/main/prompb/remote.proto) and [types.proto](https://github.com/prometheus/prometheus/blob/main/prompb/types.proto).
