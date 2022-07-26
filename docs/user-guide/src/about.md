
CeresDB is a timeseries database. However, CeresDB's goal is to handle both timeseries and analytic workloads compared with the traditional ones, which usually have a poor performance in handling analytic workloads.

# Motivation
In the traditional timeseries database, the `Tag` columns (InfluxDB calls them `Tag` and Prometheus calls them `Label`) are normally indexed by generating an inverted index. However, it is found that the cardinality of `Tag` varies in different scenarios. And in some scenarios the cardinality of `Tag` is very high, and it takes a very high cost to store and retrieve the inverted index. On the other hand, it is observed that scanning+pruning often used by the analytical databases can do a good job to handle such these scenarios.

The basic design idea of CeresDB is to adopt a hybrid storage format and the corresponding query method for a better performance in processing both timeseries and analytic workloads.

# How does CeresDB work?
- See [Quick Start](quick_start.md) to learn about how to get started
- For data model of CeresDB, see [Data Model](model)
- For the supported SQL data types, operators, and commands, please navigate to [SQL reference](sql)
