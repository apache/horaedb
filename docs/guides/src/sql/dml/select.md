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

### SELECT Clause
`SELECT` is used to query rows from one or more tables, Here is an Example:

```sql
SELECT * FROM `demo` LIMIT 10
```

### FROM Clause
`FROM` is used to query rows from one or more tables, Here is an Example:

```sql
SELECT * FROM `demo` LIMIT 10
```

### WHERE Clause
`WHERE` is used to filter data rows that must satisfy to be selected, `where_condition` is an expression that evaluates to true for each row to be selected. Here is an Example:

```sql
SELECT * FROM `demo` where time_stamp > '2022-10-11 00:00:00' and time_stamp < '2022-10-12 00:00:00' LIMIT 10
```

### WHERE Clause
`WHERE` is used to filter data rows that must satisfy to be selected, `where_condition` is an expression that evaluates to true for each row to be selected. Here is an Example:

```sql
SELECT * FROM `demo` where time_stamp > '2022-10-11 00:00:00' and time_stamp < '2022-10-12 00:00:00' LIMIT 10
```

