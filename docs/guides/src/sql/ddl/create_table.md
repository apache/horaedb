# CREATE TABLE

## Basic syntax

Basic syntax (parts between `[]` are optional):
```sql
CREATE TABLE [IF NOT EXIST] 
    table_name ( column_definitions )
    [partition_options]
    ENGINE = engine_type 
    [WITH ( table_options )];
```

Column definition syntax:
```sql
column_name column_type [[NOT] NULL] {[TAG] | [TIMESTAMP KEY] | [PRIMARY KEY]}
```

Partition options syntax:
```sql

PARTITION BY KEY (column_list) [PARTITIONS num]
```

Table options syntax are key-value pairs. Value should be quoted with quotation marks (`'`). E.g.:
```sql
... WITH ( enable_ttl='false' )
```

## IF NOT EXIST

Add `IF NOT EXIST` to tell CeresDB to ignore errors if the table name already exists.

## Define Column

A column's definition should at least contains the name and type parts. All supported types are listed [here](../model/data_types.md).

Column is default be nullable. i.e. `NULL` keyword is implied. Adding `NOT NULL` constrains to make it required.
```sql
-- this definition
a_nullable int
-- equals to
a_nullable int NULL

-- add NOT NULL to make it required
b_not_null NOT NULL
```

A column can be marked as [special column](../model/special_columns.md) with related keyword.

## Engine

Specifies which engine this table belongs to. CeresDB current support [`Analytic`](../../analytic_engine/README.md) engine type. This attribute is immutable.

## Partition Options

`partition_options` can be used to control partitioning of the table. This example shows a table partitioned by key, with 8 partitions:
```sql
CREATE TABLE `demo` (`name` string TAG, `value` double NOT NULL, `t` timestamp NOT NULL, TIMESTAMP KEY(t))
    PARTITION BY KEY(name) PARTITIONS 8
    ENGINE=Analytic with (enable_ttl='false')
```