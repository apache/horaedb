Prometheus read extension for CeresDB 
---------------------------

- Feature Name: prometheus-read-extension
- Tracking Issue: https://github.com/CeresDB/ceresdb/issues/90

# Summary
Drop-in and full-featured Prometheus read extension for CeresDB

# Motivation
Prometheus and PromQL are wide used in monitoring scenarios. It would be great if CeresDB can be queried using PromQL. 

CeresDB has the ability to store and compute a large amount of data. But PromQL contains some specific operators. Though CeresDB supports a subset, it is hard and infeasible to implement all of them.

There are some brilliant distributed solutions like `Thanos` and `Cortex`. But the computation ability is limited in aspects of distributed execution or extensible (`Thanos` supports split query on time range (https://thanos.io/tip/components/query-frontend.md/#splitting). Combining `Prometheus` with `CeresDB` can gain both high performance computation and the ability to query in other forms like SQL.  

This proposal aims to provide a way that:

- Easy to integrate with Prometheus server without modifying it.
- Able to offload computation to CeresDB.

# Details
The proposed architecture contains three parts `Prometheus`, `CeresDB`, and `Query Frontend`. They can be combined in two ways.

One is to deploy them together in a single node. This node provides Prometheus service via `Query Frontend` and the other two components are internal. This requires `CeresDB` server to implement `Prometheus`' remote storage interface, and can communicate with `Query Frontend`.

```plaintext
                Prometheus
                Protocol
                    ▲
                    │
 ┌──────────────────┼──────────────────┐
 │                  │                  │
 │        ┌─────────┴──────────┐       │
 │        │   Query Frontend   │       │
 │        └─┬─▲──────────────┬─┘       │
 │          │ │              │         │
 │     sub- │ │query         │CeresDB  │
 │    PromQL│ │result        │  AST    │
 │    (HTTP)│ │              │ (gRPC)  │ ┌──────────┐
 │ ┌────────▼─┴─┐        ┌───▼─────┐   │ │ CeresDB  │
 │ │ Prometheus ◄────────► CeresDB ├───┼─┤ Storage  │
 │ └────────────┘ Remote └─────────┘   │ │(WAL/OSS) │
 │                Storage              │ └──────────┘
 │Compute         Interface            │
 │Node            (Read)               │
 └─────────────────────────────────────┘
```

The other way is only to include `Query Frontend` and `Prometheus` inside one node, and they query `CeresDB` cluster through a normal client interface. In this case the assembled compute node is kind of "special client" to `CeresDB`.

```plaintext
           Prometheus
           Protocol
              ▲
              │
 ┌────────────┼────────────────┐
 │            │                │
 │   ┌────────┴───────┐        │
 │   │ Query Frontend ◄────────┼─────────┐
 │   └──────┬─▲───────┘        │         │
 │          │ │                │    ┌────▼────┐
 │  PromQL  │ │Remote Read req │    │ CeresDB ├┐
 │          │ │                │    │ Cluster ││
 │  Data    │ │Compute Result  │    └─┬───────┘│
 │     ┌────▼─┴─────┐          │      └────────┘
 │     │ Prometheus │          │
 │     └────────────┘          │
 │Compute Node                 │
 └─────────────────────────────┘
```

## Query Frontend
The entry point of one deploy node. It masquerades as a `Prometheus` server (this proposal only focus on read). This is added to act as a proxy layer on top of `Prometheus` and `CeresDB` services (only in the first case).

A query frontend will analyze incoming `PromQL` query and split it into two sub-parts, then feed them to the underlying `Prometheus` and `CeresDB`. The two parts should be legal to both receivers, i.e both `Prometheus` and `CeresDB` can process it like a normal request.

Frontend should be able to parse and manipulate both PromQL and CeresDB's AST. It assembles CeresDB's AST based on the information about operators supported by CeresDB (like sum, max, min, etc.). And replace these operators with a selector, which will be computed in CeresDB later. Example:

--------------------

Input PromQL:
```
histogram_quantile(0.9, sum by (le) (rate(http_request_duration_seconds_bucket[10m])))
```
This PromQL queries p90 of metric `http_request_duration_sections_bucket`. CeresDB supports `sum` and `rate` and we can push them down to CeresDB's part. `histogram_quantile` is not supported thus it remains in PromQL and is calculated by Prometheus. So the two parts derived from this example are:
```promql
histogram_quantile(0.9, sum_rate_http_result)
```
and
```sql
SELECT
    -- rate is an UDF in CeresDB
    sum(rate('10m', field)) AS sum_rate_http_result
FROM http_request_duration_seconds_bucket
WHERE timestamp BETWEEN A AND B
GROUP BY le
ORDER BY timestamp
```
------------------------------

Frontend also needs to forward query result from Prometheus.

## Communication

### Embedded CeresDB Server
In the first case `CeresDB` is integrated into `Prometheus` as a remote (read) storage within one node. `Prometheus`' remote request protocol is defined here: https://github.com/prometheus/prometheus/blob/main/prompb/remote.proto.

Query Frontend has to feed PromQL and SQL to servers separately because this interface cannot pass query plan (except filter). The two requests are associated with a `TaskID` generated by Frontend. This ID is present in remote request and `CeresDB` can recognize it. At this point we can describe a complete flow for serving a PromQL query:

1. `Query Frontend` accepts a PromQL.
2. `Query Frontend` splits the original PromQL into two sub queries and assigns a `TaskID`.
3. `Query Frontend` sends sub PromQL to `Prometheus` and sub SQL to CeresDB. 
4. `Prometheus` processes the sub PromQL. It will query the data source (CeresDB) for data.
5. `CeresDB` receives a request from `Prometheus`, and a sub-SQL with the same `TaskID` from `Query Frontend`.
6. `CeresDB` processes and returns result to `Prometheus`.
7. `Prometheus` processes and returns result.
8. `Query Frontend` forwards results to the client.

<!---
@startuml
actor Client
participant Query_Frontend
participant Prometheus
database CeresDB

Client -> Query_Frontend : PromQL request
Query_Frontend -> Prometheus : sub PromQL request with TaskID
Query_Frontend -> CeresDB : sub SQL request with TaskID
Prometheus -> CeresDB : remote storage read with TaskID
CeresDB -> CeresDB : pull data and compute
CeresDB -> Prometheus : response remote read request
Prometheus -> Prometheus : compute
Prometheus -> Query_Frontend: response PromQL request
Query_Frontend -> Client : response PromQL request
@enduml
-->

```plaintext
                                                                                               ,.-^^-._                       
        ,-.                                                                                   |-.____.-|                      
        `-'                                                                                   |        |                      
        /|\                                                                                   |        |                      
         |                ,--------------.                  ,----------.                      |        |                      
        / \               |Query_Frontend|                  |Prometheus|                      '-.____.-'                      
      Client              `------+-------'                  `----+-----'                       CeresDB                        
        |     PromQL request     |                               |                                |                           
        | ----------------------->                               |                                |                           
        |                        |                               |                                |                           
        |                        | sub PromQL request with TaskID|                                |                           
        |                        | ------------------------------>                                |                           
        |                        |                               |                                |                           
        |                        |                   sub SQL request with TaskID                  |                           
        |                        | --------------------------------------------------------------->                           
        |                        |                               |                                |                           
        |                        |                               | remote storage read with TaskID|                           
        |                        |                               | ------------------------------->                           
        |                        |                               |                                |                           
        |                        |                               |                                |----.                      
        |                        |                               |                                |    | pull data and compute
        |                        |                               |                                |<---'                      
        |                        |                               |                                |                           
        |                        |                               |  response remote read request  |                           
        |                        |                               | <-------------------------------                           
        |                        |                               |                                |                           
        |                        |                               |----.                           |                           
        |                        |                               |    | compute                   |                           
        |                        |                               |<---'                           |                           
        |                        |                               |                                |                           
        |                        |    response PromQL request    |                                |                           
        |                        | <------------------------------                                |                           
        |                        |                               |                                |                           
        | response PromQL request|                               |                                |                           
        | <-----------------------                               |                                |                           
      Client              ,------+-------.                  ,----+-----.                       CeresDB                        
        ,-.               |Query_Frontend|                  |Prometheus|                       ,.-^^-._                       
        `-'               `--------------'                  `----------'                      |-.____.-|                      
        /|\                                                                                   |        |                      
         |                                                                                    |        |                      
        / \                                                                                   |        |                      
                                                                                              '-.____.-'                      
```

### Separated CeresDB cluster
The second mode moves `CeresDB` out and makes `Query Frontend` as the remote storage to `Prometheus`. Instead of sending a sub-SQL plan to `CeresDB`, `Query Frontend` keeps the SQL and uses it to query `CeresDB` when a remote read request from `Prometheus` arrives. By doing so we can communicate with `CeresDB` cluster with a usual client interface, and manage all the states inside `Query Frontend`. The flow is:

1. `Query Frontend` accepts a PromQL.
2. `Query Frontend` splits the original PromQL into two sub queries.
3. `Query Frontend` sends sub PromQL to `Prometheus`.
4. `Prometheus` processes the sub PromQL. The data request is sent back to `Query Frontend`.
5. `Query Frontend` receives the remote storage request. Then queries `CeresDB` use the corresponding sub SQL.
6. `CeresDB` returns the query result.
7. `Query Frontend` forwards result to `Prometheus`. It may need to transform the data format.
8. `Prometheus` processes and return result.
9. `Query Frontend` forwards result to the client.

<!---
@startuml
actor Client
participant Query_Frontend
participant Prometheus
database CeresDB

Client -> Query_Frontend : PromQL request
Query_Frontend -> Prometheus : sub PromQL request
Query_Frontend -> Query_Frontend : store the sub SQL
Prometheus -> Query_Frontend : remote storage read
Query_Frontend -> CeresDB : query sub SQL using CeresDB Client
CeresDB -> Query_Frontend : sub SQL query result
Query_Frontend -> Query_Frontend : transform data format
Query_Frontend -> Prometheus : response remote read request
Prometheus -> Prometheus : compute
Prometheus -> Query_Frontend : response sub PromQL request
Query_Frontend -> Client : response PromQL request
@enduml
-->

```plaintext
                                                                                 ,.-^^-._ 
        ,-.                                                                     |-.____.-|
        `-'                                                                     |        |
        /|\                                                                     |        |
         |                ,--------------.                ,----------.          |        |
        / \               |Query_Frontend|                |Prometheus|          '-.____.-'
      Client              `------+-------'                `----+-----'           CeresDB  
        |     PromQL request     |                             |                    |     
        | ----------------------->                             |                    |     
        |                        |                             |                    |     
        |                        |      sub PromQL request     |                    |     
        |                        | ---------------------------->                    |     
        |                        |                             |                    |     
        |                        |----.                        |                    |     
        |                        |    | store the sub SQL      |                    |     
        |                        |<---'                        |                    |     
        |                        |                             |                    |     
        |                        |     remote storage read     |                    |     
        |                        | <----------------------------                    |     
        |                        |                             |                    |     
        |                        |        query sub SQL using CeresDB Client        |     
        |                        | ------------------------------------------------>|     
        |                        |                             |                    |     
        |                        |               sub SQL query result               |     
        |                        | <------------------------------------------------|     
        |                        |                             |                    |     
        |                        |----.                        |                    |     
        |                        |    | transform data format  |                    |     
        |                        |<---'                        |                    |     
        |                        |                             |                    |     
        |                        | response remote read request|                    |     
        |                        | ---------------------------->                    |     
        |                        |                             |                    |     
        |                        |                             |----.               |     
        |                        |                             |    | compute       |     
        |                        |                             |<---'               |     
        |                        |                             |                    |     
        |                        | response sub PromQL request |                    |     
        |                        | <----------------------------                    |     
        |                        |                             |                    |     
        | response PromQL request|                             |                    |     
        | <-----------------------                             |                    |     
      Client              ,------+-------.                ,----+-----.           CeresDB  
        ,-.               |Query_Frontend|                |Prometheus|           ,.-^^-._ 
        `-'               `--------------'                `----------'          |-.____.-|
        /|\                                                                     |        |
         |                                                                      |        |
        / \                                                                     |        |
                                                                                '-.____.-'
```

## Comparison
Both ways can achieve our initial requirements and are able to implement distributed execution in the future. 

- Embedded `CeresDB`
    - Pros.
        - `CeresDB` feeds data to `Prometheus` directly, reducing some computation and transmission.
    - Cons.
        - Need to customize a `Prometheus` specific interface in `CeresDB`. 
        - The deployment may requires all three components bound together for simplicity.
- Separated `CeresDB` cluster
    - Pros.
        - No need to customize `CeresDB` because `Prometheus` extension is decoupled with it in both develop and deploy aspects.
        - The deployment only requires one `Query Frontend` along with `Prometheus` which is more lightweight and less invasive.
        - States of `CeresDB` and `Query Frontend` are simple and clear.
    - Cons.
        - One more data transforming and forwarding in `Query Frontend` (pass results from `CeresDB` to `Prometheus`). 

# Drawbacks
Detailed in the "Comparison" section above.

# Alternatives
- Modify `Prometheus` rather than using the native upstream version. By doing this we can
   - Extend the remote storage API to pass SQL sub-query
   - Merge `Prometheus` and `Query Frontend` into one component.
