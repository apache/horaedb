# Special Columns

Tables in CeresDB have the following constraints:
* Primary key is required
* The primary key must contain a time column, and can only contain one time column
* The primary key must be non-null, so all columns in primary key must be non-null.

## Timestamp Column

Tables in CeresDB must have one timestamp column maps to timestamp in timeseries data, such as timestamp in OpenTSDB/Prometheus.
The timestamp column can be set with `timestamp key` keyword, like `TIMESTAMP KEY(ts)`.

## Tag column

`Tag` is use to defined column as tag column, similar to tag in timeseries data, such as tag in OpenTSDB and label in Prometheus.

## Primary key

The primary key is used for data deduplication and sorting. The primary key is composed of some columns and one time column.
The primary key can be set in the following some ways：
* use `primary key` keyword
* use `tag` to auto generate TSID, CeresDB will use `(timestamp,TSID)` as primary key
* only set Timestamp column, CeresDB will use `(timestamp)` as primary key

Notice: If the primary key and tag are specified at the same time, then the tag column is just an additional information identification and will not affect the logic.

``` sql
CREATE TABLE with_primary_key(
  ts TIMESTAMP NOT NULL,
  c1 STRING NOT NULL,
  c2 STRING NULL,
  c4 STRING NULL,
  c5 STRING NULL,
  TIMESTAMP KEY(ts),
  PRIMARY KEY(c1, ts)
) ENGINE=Analytic WITH (ttl='7d');
  
CREATE TABLE with_tag(
    ts TIMESTAMP NOT NULL,
    c1 STRING TAG NOT NULL,
    c2 STRING TAG NULL,
    c3 STRING TAG NULL,
    c4 DOUBLE NULL,
    c5 STRING NULL,
    c6 STRING NULL,
    TIMESTAMP KEY(ts)
) ENGINE=Analytic WITH (ttl='7d');

CREATE TABLE with_timestamp(
    ts TIMESTAMP NOT NULL,
    c1 STRING NOT NULL,
    c2 STRING NULL,
    c3 STRING NULL,
    c4 DOUBLE NULL,
    c5 STRING NULL,
    c6 STRING NULL,
    TIMESTAMP KEY(ts)
) ENGINE=Analytic WITH (ttl='7d');
```

## TSID

If `primary key`is not set, and tag columns is provided, TSID will auto generated from hash of tag columns. 
In essence, this is also a mechanism for automatically generating id.


