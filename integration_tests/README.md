# Integration tests suite for HoraeDB

## Running test

There are three Makefile commands to run integration test:
```sh
# All envs
make run

# Only local env
make run-local

# Only cluster env
make run-cluster

# Only compaction offload env
make run-compaction-offload
```

`horaedb-test` will recursively find all the files end with `.sql` and run it. Each file will be treated as a case. A file can contain multiple SQLs. When finished it will tell how many cases it run, and display the diff set if there is any. An example with one case:
```
Server from "/path/to/horaedb/target/debug/horaedb-server" is starting ...
Takes 49.020203ms. Diff: false. Test case "/path/to/horaedb/tests/cases/example.sql" finished.
Run 1 finished. 0 cases are different.
```

Users can set `HORAEDB_ENV_FILTER` variables to filter env to run. For example:
```
HORAEDB_ENV_FILTER=local make run
```
This command will only run cases in `local`.

## Add a test

Please refer README of https://github.com/CeresDB/sqlness

## Test case organization

Cases are grouped by scenario. SQLs used to test one feature are put in one file. Like `top_k.sql` for `TopK` operator and `limit.sql` for `LIMIT` function.

On top of files, we organize them by deployment. Like `local/` contains all the cases run in a standalone server.
