# Utility Statements

There are serval utilities SQL in CeresDB that can help in table manipulation or query inspection.

## SHOW CREATE TABLE

```sql
SHOW CREATE TABLE table_name;
```

`SHOW CREATE TABLE` returns a `CREATE TABLE` DDL that will create a same table with the given one. Including columns, table engine and options. The schema and options shows in `CREATE TABLE` will based on the current version of the table. An example:

```sql
-- create one table
CREATE TABLE `t` (a bigint, b int default 3, c string default 'x', d smallint null, t timestamp NOT NULL, TIMESTAMP KEY(t)) ENGINE = Analytic;
-- Result: affected_rows: 0

-- show how one table should be created.
SHOW CREATE TABLE `t`;

-- Result DDL:
CREATE TABLE `t` (
    `t` timestamp NOT NULL,
    `tsid` uint64 NOT NULL,
    `a` bigint,
    `b` int,
    `c` string,
    `d` smallint,
    PRIMARY KEY(t,tsid),
    TIMESTAMP KEY(t)
) ENGINE=Analytic WITH (
    arena_block_size='2097152',
    compaction_strategy='default',
    compression='ZSTD',
    enable_ttl='true',
    num_rows_per_row_group='8192',
    segment_duration='',
    ttl='7d',
    update_mode='OVERWRITE',
    write_buffer_size='33554432'
)
```

## DESCRIBE

```sql
DESCRIBE table_name;
```

`DESCRIBE` will show a detailed schema of one table. The attributes include column name and type, whether it is tag and primary key (todo: ref) and whether it's nullable. The auto created column `tsid` will also be included (todo: ref).

Example:

```sql
CREATE TABLE `t`(a int, b string, t timestamp NOT NULL, TIMESTAMP KEY(t)) ENGINE = Analytic;

DESCRIBE TABLE `t`;
```

The result is:
```
name    type        is_primary  is_nullable is_tag

t       timestamp   true        false       false
tsid    uint64      true        false       false
a       int         false       true        false
b       string      false       true        false
```

## EXPLAIN

```sql
EXPLAIN query;
```

`EXPLAIN` shows how a query will be executed. Add it to the beginning of a query like

```sql
EXPLAIN SELECT max(value) AS c1, avg(value) AS c2 FROM `t` GROUP BY name;
```

will give

```
logical_plan
Projection: #MAX(07_optimizer_t.value) AS c1, #AVG(07_optimizer_t.value) AS c2
  Aggregate: groupBy=[[#07_optimizer_t.name]], aggr=[[MAX(#07_optimizer_t.value), AVG(#07_optimizer_t.value)]]
    TableScan: 07_optimizer_t projection=Some([name, value])

physical_plan
ProjectionExec: expr=[MAX(07_optimizer_t.value)@1 as c1, AVG(07_optimizer_t.value)@2 as c2]
  AggregateExec: mode=FinalPartitioned, gby=[name@0 as name], aggr=[MAX(07_optimizer_t.value), AVG(07_optimizer_t.value)]
    CoalesceBatchesExec: target_batch_size=4096
      RepartitionExec: partitioning=Hash([Column { name: \"name\", index: 0 }], 6)
        AggregateExec: mode=Partial, gby=[name@0 as name], aggr=[MAX(07_optimizer_t.value), AVG(07_optimizer_t.value)]
          ScanTable: table=07_optimizer_t, parallelism=8, order=None
```