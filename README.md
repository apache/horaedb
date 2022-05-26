# ceresdbx

## Building
Install clang (for rocksdb)

Install deps (required by rust-rocksdb)
```bash
brew install cmake
brew install lz4
```

Build in debug mode
```bash
cargo build --bin ceresdb-server
```

Build in release mode
```bash
cargo build --release --bin ceresdb-server
```

## Usage
Run the server
```bash
./ceresdb-server
```

## RESTful API
```bash
curl -L -X POST 'http://localhost:5000/sql' \
-H 'Content-Type: application/json' \
-d '{
    "query": "your DDL sql"
}'
```

Describe a table
```bash
curl -L -X POST 'http://localhost:5000/sql' \
-H 'Content-Type: application/json' \
-d '{
    "query": "DESCRIBE TABLE mytest"
}'
```

Insert data
```bash
curl -L -X POST 'http://localhost:5000/sql' \
-H 'Content-Type: application/json' \
--data-raw '{
    "query": "INSERT INTO mytest(c1, c2, c3, c4, c5, c6) VALUES(1618310218001, 12.5, '\''hello world'\'', 3.14159265, true, 2147483650)"
}'
```

Query
```bash
curl -L -X POST 'http://localhost:5000/sql' \
-H 'Content-Type: application/json' \
-d '{
    "query": "SELECT c1, c2, c3, c4, c5, c6 FROM mytest LIMIT 3"
}'
```

Query from system tables
```bash
curl -L -X POST 'http://localhost:5000/sql' \
-H 'Content-Type: application/json' \
-d '{
    "query": "SELECT * FROM system.numbers LIMIT 3"
}'
```

## Support Data Type
| SQL | CeresDB | Arrow |
| --- | --- | --- |
| null | Null | Null |
| timestamp | Timestamp | Timestamp(TimeUnit::Millisecond, None) |
| double | Double | Float64 |
| float | Float | Float32 |
| string | String | String |
| Varbinary | Varbinary | Binary |
| uint64 | UInt64 | UInt64 |
| uint32 | UInt32 | UInt32 |
| uint16 | UInt16 | UInt16 |
| uint8 | UInt8 | UInt8 |
| int64/bigint | Int64 | Int64 |
| int32/int | Int32 | Int32 |
| int16/smallint | Int16 | Int16 |
| int8/tinyint | Int8 | Int8 |
| boolean | Boolean | Boolean |
