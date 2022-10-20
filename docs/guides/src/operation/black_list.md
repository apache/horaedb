# Black list

## Read black list
If you want to reject to query to a table, you can add table to 'read_reject_list'.

### Example
```
curl --location --request POST 'http://localhost:5000/reject' \
--header 'Content-Type: application/json' \
-d '{
    "operation":"Add",
    "write_reject_list":[],
    "read_reject_list":["my_table"]
}'
```

## Write black list

If you want to reject to write to a table, you can add table to 'write_reject_list'

### Example

```
curl --location --request POST 'http://localhost:5000/reject' \
--header 'Content-Type: application/json' \
-d '{
    "operation":"Add",
    "write_reject_list":["my_table"],
    "read_reject_list":[]
}'
```
