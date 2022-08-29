# Options

Options below can be used when create table for analytic engine

- `enable_ttl`, `bool`. When enable TTL on a table, rows older than `ttl` will be deleted and can't be querid, default `true`
- `ttl`, `duration`, lifetime of a row, only used when `enable_ttl` is `true`. default `7d`.
- `storage_format`, `string`. The underlying column's format. Availiable values:
  - `columnar`, default
  - `hybrid`

  The meaning of those two values are in [Storage format](#storage-format) section.


## Storage Format

There are mainly two formats supported in analytic engine. One is `columnar`, which is the traditional columnar format, with one table column in one physical column:

```plaintext
| Timestamp | Device ID | Status Code | Tag 1 | Tag 2 |
| --------- |---------- | ----------- | ----- | ----- |
| 12:01     | A         | 0           | v1    | v1    |
| 12:01     | B         | 0           | v2    | v2    |
| 12:02     | A         | 0           | v1    | v1    |
| 12:02     | B         | 1           | v2    | v2    |
| 12:03     | A         | 0           | v1    | v1    |
| 12:03     | B         | 0           | v2    | v2    |
| .....     |           |             |       |       |
```

The other one is `hybrid`, an experimental format used to simulate row-oriented storage in columnar storage to accelerate traditional time-series query.

In traditional time-series user cases like IoT or DevOps, queries will typically first group their result by series id(or device id), then by timestamp. In order to achieve good performance in those scenarios, the data physical layout should match this style, so the `hybrid` format is proposed like this:


```plaintext
 | Device ID | Timestamp           | Status Code | Tag 1 | Tag 2 | minTime | maxTime |
 |-----------|---------------------|-------------|-------|-------|---------|---------|
 | A         | [12:01,12:02,12:03] | [0,0,0]     | v1    | v1    | 12:01   | 12:03   |
 | B         | [12:01,12:02,12:03] | [0,1,0]     | v2    | v2    | 12:01   | 12:03   |
 | ...       |                     |             |       |       |         |         |
 ```


- Within one file, rows belonging to the same primary key(eg: series/device id) are collapsed into one row
- The columns besides primary key are divided into two categories:
  - `collapsible`, those columns will be collapsed into a list. Used to encode `fields` in time-series table
    - Note: only fixed-length type is supported now
  - `non-collapsible`, those columns should only contain one distinct value. Used to encode `tags` in time-series table
    - Note: only string type is supported now
- Two more columns are added, `minTime` and `maxTime`. Those are used to cut unnecessary rows out in query.
  - Note: Not implemented yet.

### Example

```sql
CREATE TABLE `device` (
    `ts` timestamp NOT NULL,
    `tag1` string tag,
    `tag2` string tag,
    `value1` double,
    `value2` int,
    timestamp KEY (ts)) ENGINE=Analytic
  with (
    enable_ttl = 'false',
    storage_format = 'hybrid'
);
```
This will create a table with hybrid format, users can inspect data format with [parquet-tools](https://formulae.brew.sh/formula/parquet-tools). The table above should have following parquet schema:

```
message arrow_schema {
  optional group ts (LIST) {
    repeated group list {
      optional int64 item (TIMESTAMP(MILLIS,false));
    }
  }
  required int64 tsid (INTEGER(64,false));
  optional binary tag1 (STRING);
  optional binary tag2 (STRING);
  optional group value1 (LIST) {
    repeated group list {
      optional double item;
    }
  }
  optional group value2 (LIST) {
    repeated group list {
      optional int32 item;
    }
  }
}
```
