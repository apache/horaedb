# Table Operation

## Query Table Information
Like Mysql's `information_schema.tables`, Ceresdb provides `system.public.tables` to save tables information.
Columns:
* timestamp([TimeStamp](./xx))
* catalog([String](./xx))
* schema([String](./xx))
* table_name([String](./xx))
* table_id([Uint64](./xx))
* engine([String](./xx))

### Example

Query table information via table_name like this:

```
curl --location --request POST 'http://localhost:5000/sql' \
--header 'Content-Type: application/json' \
--header 'x-ceresdb-access-tenant: my_tenant' \
-d '{
    "query": "select * from system.public.tables where `table_name`=\"my_table\""
}'
```
### Response
```
{
    "rows":[
        {
            "timestamp":0,
            "catalog":"ceresdb",
            "schema":"monitor_trace",
            "table_name":"my_table",
            "table_id":3298534886446,
            "engine":"Analytic"
        }
}
```