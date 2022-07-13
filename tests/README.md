# Integration tests suite for CeresDB

## Running test

First the harness needs to be compiled. Run `cargo build --release` under this folder. It will be compiled to `ceresdb-test` under `$REPO/target/release`.

The binary takes these env variables to run:
- `BINARY_PATH_ENV`: The binary to test.
- `SERVER_ENDPOINT_ENV`: Where server is set. The default gRPC port is 8831.
- `CASE_ROOT_PATH_ENV`: The cases directory. Usually this should be pointed to `tests/cases`

The command to run the test looks like
```bash
CERESDB_BINARY_PATH="./target/debug/ceresdb-server" CERESDB_SERVER_ENDPOINT="127.0.0.1:8831" CERESDB_TEST_CASE_PATH="./tests/cases" ./target/release/ceresdb-test
```

Make sure the `diff(1)` command is available in your environment. `ceresdb-test` will use it to compare the output against the expected.

`ceresdb-test` will recursively find all the files end with `.sql` and run it. Each file will be treated as a case. A file can contain multiple SQLs. When finished it will tell how many cases it run, and display the diff set if there is any. An example with one case:
```
Server from "/home/ruihang/repo/CeresDB/target/debug/ceresdb-server" is starting ...
Takes 49.020203ms. Diff: false. Test case "/home/ruihang/repo/CeresDB/tests/cases/example.sql" finished.
Run 1 finished. 0 cases are different.
```

Cases with different outputs will generate a `.out` file, which contains the actual output it gets from the server. You should make sure there is no new `.out` file after running.

## Add a test

A case is made up of two parts: the input SQL file `.sql` and the expected result file `.result`. To add a case, you can:
- Write the `.sql` file
- Run the test and get a `.out` for output
- Check the `.out` carefully to see if the result is correct
- If the `.out` is right, rename it to `.result` to make it the standard expected.
- Otherwise go back to the code to see if there is any bug.

And there are a few things that need to be mentioned:
- In most cases, you shouldn't manually edit either `.out` or `.result` file. They are and preferably should be generated.
- You can write any legal or illegal SQLs in your `.sql` file. But just remember to recover any change you have made. Like drop all the tables created in the cases. Or it would affect the next case.

## Test case organization

Cases are grouped by scenario. SQLs used to test one feature are put in one file. Like `top_k.sql` for `TopK` operator and `limit.sql` for `LIMIT` function.

On top of files, we organize them by deployment. Like `local/` contains all the cases run in a standalone server.
