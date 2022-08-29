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

The other one is `hybrid`, a exprimental format used to simulate a row-oriented storage in columnar storage, in order to accelerate OLTP queries.

In traditional time-series user case like IoT or DevOps, queries will typically first group its result by series id(or device-id), then by timestamp. In order to archieve good performance in those scenarios, the data physical layout should match this style, so the `hybrid` format is proposed like this:


```plaintext
 | Device ID | Timestamp           | Status Code | Tag 1 | Tag 2 | minTime | maxTime |
 |-----------|---------------------|-------------|-------|-------|---------|---------|
 | A         | [12:01,12:02,12:03] | [0,0,0]     | v1    | v1    | 12:01   | 12:03   |
 | B         | [12:01,12:02,12:03] | [0,1,0]     | v2    | v2    | 12:01   | 12:03   |
 | ...       |                     |             |       |       |         |         |
 ```


- With in one file, rows belong to same primary key(eg: series/device id) are collapsed in one row
- The columns besides primary key are divided in two categroies:
  1. `collapsible`, those columns will be collapsed into a list. Used to encode `fields` in time-series table
  2. `non-collapsible`, those columns should only contains one distinct value. Used to encode `tags` in time-series table
- Two more columns are added, `minTime` and `maxTime`. Those are used to cut unnecessary rows out in query.
  - Note: Not implemented yet.
