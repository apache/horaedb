# Block List

## Add block list
If you want to reject query for a table, you can add table name to 'read_block_list'.

### Example
```shell
curl --location --request POST 'http://localhost:5000/block' \
--header 'Content-Type: application/json' \
-d '{
    "operation":"Add",
    "write_block_list":[],
    "read_block_list":["my_table"]
}'
```
### Response
```json
{
  "write_block_list":[

  ],
  "read_block_list":[
    "my_table"
  ]
}
```

## Set block list

You can use set operation to clear exist tables and set new tables to 'read_block_list' like following example.

### Example
```shell
curl --location --request POST 'http://localhost:5000/block' \
--header 'Content-Type: application/json' \
-d '{
    "operation":"Set",
    "write_block_list":[],
    "read_block_list":["my_table1","my_table2"]
}'
```

### Response

```json
{
  "write_block_list":[

  ],
  "read_block_list":[
    "my_table1",
    "my_table2"
  ]
}
```

## Remove block list

You can remove tables from  'read_block_list' like following example.

### Example

```shell
curl --location --request POST 'http://localhost:5000/block' \
--header 'Content-Type: application/json' \
-d '{
    "operation":"Remove",
    "write_block_list":[],
    "read_block_list":["my_table1"]
}'
```

### Response

```json
{
  "write_block_list":[

  ],
  "read_block_list":[
    "my_table2"
  ]
}
```