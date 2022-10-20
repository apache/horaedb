# 表操作

## 查询表信息
通过table_id查询表信息
```
curl --location --request POST 'http://localhost:5000/sql' \
--header 'Content-Type: application/json' \
--header 'x-ceresdb-access-tenant: my_tenant' \
-d '{
    "query": "select * from system.public.tables where `table_id`=7696581396722"
}
'
```

通过table_name查询表信息
```
curl --location --request POST 'http://localhost:5000/sql' \
--header 'Content-Type: application/json' \
--header 'x-ceresdb-access-tenant: my_tenant' \
-d '{
    "query": "select * from system.public.tables where `table_name`=\"my_table\""
}'
```

## 删除表
删除指定表
```
curl --location --request POST 'http://localhost:5000/sql' \
--header 'Content-Type: application/json' \
-d'{
    "query": "drop table ceresdb.my_tenant.my_table'"
}'
```

## 黑名单
把表加入读黑名单
```
curl --location --request POST 'http://localhost:5000/reject' \
--header 'Content-Type: application/json' \
-d '{
    "operation":"Add",
    "write_reject_list":[],
    "read_reject_list":["my_table"]
}'
```
把表加入写黑名单
```
curl --location --request POST 'http://localhost:5000/reject' \
--header 'Content-Type: application/json' \
-d '{
    "operation":"Add",
    "write_reject_list":["my_table"],
    "read_reject_list":[]
}'
```