# SELECT

## Basic syntax

Basic syntax (parts between `[]` are optional):

```sql
SELECT select_expr [, select_expr] ...
    FROM table_name
    [WHERE where_condition]
    [GROUP BY {col_name | expr}... ]
    [ORDER BY {col_name | expr}
    [ASC | DESC]
    [LIMIT [offset,] row_count ]
```

`Select` syntax in CeresDB is similar to mysql, here is an example:

```sql
SELECT * FROM `demo` WHERE time_stamp > '2022-10-11 00:00:00' AND time_stamp < '2022-10-12 00:00:00' LIMIT 10
```


