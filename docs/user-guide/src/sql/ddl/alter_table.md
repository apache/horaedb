# ALTER TABLE

`ALTER TABLE` can change the schema or options of a table.

CeresDB current supports `ADD COLUMN`.

```sql
-- create a table and add a column to it
CREATE TABLE `t`(a int, t timestamp NOT NULL, TIMESTAMP KEY(t)) ENGINE = Analytic;
ALTER TABLE `t` ADD COLUMN (b string);
```

It now becomes
```
-- DESCRIBE TABLE `t`;

name    type        is_primary  is_nullable is_tag

t       timestamp   true        false       false
tsid    uint64      true        false       false
a       int         false       true        false
b       string      false       true        false
```
