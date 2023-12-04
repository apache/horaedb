Prometheus read extension for HoraeDb 
---------------------------

- Feature Name: prometheus-read-extension
- Tracking Issue: https://github.com/CeresDB/horaedb/issues/90

# Summary
Drop-in and full-featured Prometheus read extension for HoraeDb

# Motivation
Prometheus and PromQL are wide used in monitoring scenarios. It would be great if HoraeDb can be queried using PromQL. 

HoraeDb has the ability to store and compute a large amount of data. But PromQL contains some specific operators. Though HoraeDb supports a subset, it is hard and infeasible to implement all of them.

There are some brilliant distributed solutions like `Thanos` and `Cortex`. But the computation ability is limited in aspects of distributed execution or extensible (`Thanos` supports split query on time range (https://thanos.io/tip/components/query-frontend.md/#splitting). Combining `Prometheus` with `HoraeDb` can gain both high performance computation and the ability to query in other forms like SQL.  

This proposal aims to provide a way that:

- Easy to integrate with Prometheus server without modifying it.
- Able to offload computation to HoraeDb.

# Details
The proposed architecture contains three parts `Prometheus`, `HoraeDb`, and `Query Frontend`. They can be combined in two ways.

One is to deploy them together in a single node. This node provides Prometheus service via `Query Frontend` and the other two components are internal. This requires `HoraeDb` server to implement `Prometheus`' remote storage interface, and can communicate with `Query Frontend`.

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
 │     sub- │ │query         │HoraeDb  │
 │    PromQL│ │result        │  AST    │
 │    (HTTP)│ │              │ (gRPC)  │ ┌──────────┐
 │ ┌────────▼─┴─┐        ┌───▼─────┐   │ │ HoraeDb  │
 │ │ Prometheus ◄────────► HoraeDb ├───┼─┤ Storage  │
 │ └────────────┘ Remote └─────────┘   │ │(WAL/OSS) │
 │                Storage              │ └──────────┘
 │Compute         Interface            │
 │Node            (Read)               │
 └─────────────────────────────────────┘
```

The other way is only to include `Query Frontend` and `Prometheus` inside one node, and they query `HoraeDb` cluster through a normal client interface. In this case the assembled compute node is kind of "special client" to `HoraeDb`.

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
 │  PromQL  │ │Remote Read req │    │ HoraeDb ├┐
 │          │ │                │    │ Cluster ││
 │  Data    │ │Compute Result  │    └─┬───────┘│
 │     ┌────▼─┴─────┐          │      └────────┘
 │     │ Prometheus │          │
 │     └────────────┘          │
 │Compute Node                 │
 └─────────────────────────────┘
```

## Query Frontend
The entry point of one deploy node. It masquerades as a `Prometheus` server (this proposal only focus on read). This is added to act as a proxy layer on top of `Prometheus` and `HoraeDb` services (only in the first case).

A query frontend will analyze incoming `PromQL` query and split it into two sub-parts, then feed them to the underlying `Prometheus` and `HoraeDb`. The two parts should be legal to both receivers, i.e both `Prometheus` and `HoraeDb` can process it like a normal request.

Frontend should be able to parse and manipulate both PromQL and HoraeDb's AST. It assembles HoraeDb's AST based on the information about operators supported by HoraeDb (like sum, max, min, etc.). And replace these operators with a selector, which will be computed in HoraeDb later. Example:

--------------------

Input PromQL:
```
histogram_quantile(0.9, sum by (le) (rate(http_request_duration_seconds_bucket[10m])))
```
This PromQL queries p90 of metric `http_request_duration_sections_bucket`. HoraeDb supports `sum` and `rate` and we can push them down to HoraeDb's part. `histogram_quantile` is not supported thus it remains in PromQL and is calculated by Prometheus. So the two parts derived from this example are:
```promql
histogram_quantile(0.9, sum_rate_http_result)
```
and
```sql
SELECT
    -- rate is an UDF in HoraeDb
    sum(rate('10m', field)) AS sum_rate_http_result
FROM http_request_duration_seconds_bucket
WHERE timestamp BETWEEN A AND B
GROUP BY le
ORDER BY timestamp
```
------------------------------

Frontend also needs to forward query result from Prometheus.

## Communication

### Embedded HoraeDb Server
In the first case `HoraeDb` is integrated into `Prometheus` as a remote (read) storage within one node. `Prometheus`' remote request protocol is defined here: https://github.com/prometheus/prometheus/blob/main/prompb/remote.proto.

Query Frontend has to feed PromQL and SQL to servers separately because this interface cannot pass query plan (except filter). The two requests are associated with a `TaskID` generated by Frontend. This ID is present in remote request and `HoraeDb` can recognize it. At this point we can describe a complete flow for serving a PromQL query:

1. `Query Frontend` accepts a PromQL.
2. `Query Frontend` splits the original PromQL into two sub queries and assigns a `TaskID`.
3. `Query Frontend` sends sub PromQL to `Prometheus` and sub SQL to HoraeDb. 
4. `Prometheus` processes the sub PromQL. It will query the data source (HoraeDb) for data.
5. `HoraeDb` receives a request from `Prometheus`, and a sub-SQL with the same `TaskID` from `Query Frontend`.
6. `HoraeDb` processes and returns result to `Prometheus`.
7. `Prometheus` processes and returns result.
8. `Query Frontend` forwards results to the client.

<!---
@startuml
actor Client
participant Query_Frontend
participant Prometheus
database HoraeDb

Client -> Query_Frontend : PromQL request
Query_Frontend -> Prometheus : sub PromQL request with TaskID
Query_Frontend -> HoraeDb : sub SQL request with TaskID
Prometheus -> HoraeDb : remote storage read with TaskID
HoraeDb -> HoraeDb : pull data and compute
HoraeDb -> Prometheus : response remote read request
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
      Client              `------+-------'                  `----+-----'                       HoraeDb                        
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
      Client              ,------+-------.                  ,----+-----.                       HoraeDb                        
        ,-.               |Query_Frontend|                  |Prometheus|                       ,.-^^-._                       
        `-'               `--------------'                  `----------'                      |-.____.-|                      
        /|\                                                                                   |        |                      
         |                                                                                    |        |                      
        / \                                                                                   |        |                      
                                                                                              '-.____.-'                      
```

### Separated HoraeDb cluster
The second mode moves `HoraeDb` out and makes `Query Frontend` as the remote storage to `Prometheus`. Instead of sending a sub-SQL plan to `HoraeDb`, `Query Frontend` keeps the SQL and uses it to query `HoraeDb` when a remote read request from `Prometheus` arrives. By doing so we can communicate with `HoraeDb` cluster with a usual client interface, and manage all the states inside `Query Frontend`. The flow is:

1. `Query Frontend` accepts a PromQL.
2. `Query Frontend` splits the original PromQL into two sub queries.
3. `Query Frontend` sends sub PromQL to `Prometheus`.
4. `Prometheus` processes the sub PromQL. The data request is sent back to `Query Frontend`.
5. `Query Frontend` receives the remote storage request. Then queries `HoraeDb` use the corresponding sub SQL.
6. `HoraeDb` returns the query result.
7. `Query Frontend` forwards result to `Prometheus`. It may need to transform the data format.
8. `Prometheus` processes and return result.
9. `Query Frontend` forwards result to the client.

<!---
@startuml
actor Client
participant Query_Frontend
participant Prometheus
database HoraeDb

Client -> Query_Frontend : PromQL request
Query_Frontend -> Prometheus : sub PromQL request
Query_Frontend -> Query_Frontend : store the sub SQL
Prometheus -> Query_Frontend : remote storage read
Query_Frontend -> HoraeDb : query sub SQL using HoraeDb Client
HoraeDb -> Query_Frontend : sub SQL query result
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
      Client              `------+-------'                `----+-----'           HoraeDb  
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
        |                        |        query sub SQL using HoraeDb Client        |     
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
      Client              ,------+-------.                ,----+-----.           HoraeDb  
        ,-.               |Query_Frontend|                |Prometheus|           ,.-^^-._ 
        `-'               `--------------'                `----------'          |-.____.-|
        /|\                                                                     |        |
         |                                                                      |        |
        / \                                                                     |        |
                                                                                '-.____.-'
```

## Comparison
Both ways can achieve our initial requirements and are able to implement distributed execution in the future. 

- Embedded `HoraeDb`
    - Pros.
        - `HoraeDb` feeds data to `Prometheus` directly, reducing some computation and transmission.
    - Cons.
        - Need to customize a `Prometheus` specific interface in `HoraeDb`. 
        - The deployment may requires all three components bound together for simplicity.
- Separated `HoraeDb` cluster
    - Pros.
        - No need to customize `HoraeDb` because `Prometheus` extension is decoupled with it in both develop and deploy aspects.
        - The deployment only requires one `Query Frontend` along with `Prometheus` which is more lightweight and less invasive.
        - States of `HoraeDb` and `Query Frontend` are simple and clear.
    - Cons.
        - One more data transforming and forwarding in `Query Frontend` (pass results from `HoraeDb` to `Prometheus`). 

# Drawbacks
Detailed in the "Comparison" section above.

# Alternatives
- Modify `Prometheus` rather than using the native upstream version. By doing this we can
   - Extend the remote storage API to pass SQL sub-query
   - Merge `Prometheus` and `Query Frontend` into one component.
