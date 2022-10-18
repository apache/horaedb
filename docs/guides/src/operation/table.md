# Table Operation

## Query Table Information
Query table information via table_id
```
curl --location --request POST 'http://localhost:5000/sql' \
--header 'Content-Type: application/json' \
--header 'x-ceresdb-access-tenant: my_tenant' \
-d '{
    "query": "select * from system.public.tables where `table_id`=7696581396722"
}
'
```

Query table information via table_name like this:

```
curl --location --request POST 'http://localhost:5000/sql' \
--header 'Content-Type: application/json' \
--header 'x-ceresdb-access-tenant: my_tenant' \
-d '{
    "query": "select * from system.public.tables where `table_name`=\"my_table\""
}'
```

## Drop Table

Drop specify table by following request

```
curl --location --request POST 'http://localhost:5000/sql' \
--header 'Content-Type: application/json' \
-d'{
    "query": "drop table ceresdb.my_tenant.my_table'"
}'
```

## Blacklist

If you want to reject to query to a table, you can add table to 'read_reject_list'.

```
curl --location --request POST 'http://localhost:5000/reject' \
--header 'Content-Type: application/json' \
-d '{
    "operation":"Add",
    "write_reject_list":[],
    "read_reject_list":["my_table"]
}'
```

If you want to reject to write to a table, you can add table to 'write_reject_list'

```
curl --location --request POST 'http://localhost:5000/reject' \
--header 'Content-Type: application/json' \
-d '{
    "operation":"Add",
    "write_reject_list":["my_table"],
    "read_reject_list":[]
}'
```
