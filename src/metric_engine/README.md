* Metric Engine

The basic write process is as follows:

```plaintext
  +----------------+
  |     start      |
  +----------------+
    |
    |
    v
+ - - - - - - - - - -+
'   Metric Engine    '
'                    '
' +----------------+ '
' | metric_manager | '
' +----------------+ '
'   |                '
'   |                '
'   v                '
' +----------------+ '
' | index_manager  | '
' +----------------+ '
'   |                '
'   |                '
'   v                '
' +----------------+ '
' |  data_manager  | '
' +----------------+ '
'                    '
+ - - - - - - - - - -+
    |
    |
    v
  +----------------+
  |      end       |
  +----------------+
```

The structure pass between different module is `Sample`, modeled after [data model](https://prometheus.io/docs/concepts/data_model/) used in prometheus.
