# Identifier

Identifier in CeresDB can be used as table name, column name etc. It cannot be preserved keywords or start with number and punctuation symbols. CeresDB allows to quote identifiers with back quotes (\`). In this case it can be any string like `00_table` or `select`.


Note: it's required to wrap column or table name in back quotes to keep them case-sensitive, such as
```sql
show create table `demo`;
show create table `DEMO`;
```
