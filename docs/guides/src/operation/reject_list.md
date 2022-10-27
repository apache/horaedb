# Reject List

## Add reject list
If you want to reject query for a table, you can add table name to 'read_reject_list'.

### Example
```shell
curl --location --request POST 'http://localhost:5000/reject' \
--header 'Content-Type: application/json' \
-d '{
    "operation":"Add",
    "write_reject_list":[],
    "read_reject_list":["my_table"]
}'
```
### Response
```json
{
  "write_reject_list":[

  ],
  "read_reject_list":[
    "my_table"
  ]
}
```

## Set reject list

You can use set operation to clear exist tables and set new tables to 'read_reject_list' like following example.

### Example
```shell
curl --location --request POST 'http://localhost:5000/reject' \
--header 'Content-Type: application/json' \
-d '{
    "operation":"Set",
    "write_reject_list":[],
    "read_reject_list":["my_table1","my_table2"]
}'
```

### Response

```json
{
  "write_reject_list":[

  ],
  "read_reject_list":[
    "my_table1",
    "my_table2"
  ]
}
```

## Remove reject list

You can remove tables from  'read_reject_list' like following example.

### Example

```shell
curl --location --request POST 'http://localhost:5000/reject' \
--header 'Content-Type: application/json' \
-d '{
    "operation":"Remove",
    "write_reject_list":[],
    "read_reject_list":["my_table1"]
}'
```

### Response

```json
{
  "write_reject_list":[

  ],
  "read_reject_list":[
    "my_table2"
  ]
}
```