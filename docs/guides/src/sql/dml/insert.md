# INSERT

## Basic syntax

Basic syntax (parts between `[]` are optional):

```sql
INSERT [INTO] tbl_name
    [(col_name [, col_name] ...)]
    { {VALUES | VALUE} (value_list) [, (value_list)] ... }
```

`INSERT` inserts new rows into a CeresDB table. Here is an example:

```sql
INSERT INTO demo(`time_stammp`, tag1) VALUES(1667374200022, 'ceresdb')
```