// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Obkv implementation.

use std::{collections::HashMap, error::Error as StdError, fmt};

use common_util::define_result;
use log::{error, info};
use obkv::{
    payloads::ObTableBatchOperation, Builder, ObTableClient, QueryResultSet, RunningMode, Table,
    TableOpResult, TableQuery, Value,
};
use snafu::{ensure, Backtrace, OptionExt, ResultExt, Snafu};

use crate::{
    config::ObkvConfig, KeyBoundary, ScanContext, ScanIter, ScanRequest, SeekKey, TableError,
    TableKv, WriteBatch, WriteContext,
};

#[cfg(test)]
mod tests;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid obkv config.\nBacktrace:\n{}", backtrace))]
    InvalidConfig { backtrace: Backtrace },

    #[snafu(display(
        "Failed to build obkv client, user:{}, err:{}.\nBacktrace:\n{}",
        full_user_name,
        source,
        backtrace
    ))]
    BuildClient {
        full_user_name: String,
        source: obkv::error::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to init obkv client, user:{}, err:{}.\nBacktrace:\n{}",
        full_user_name,
        source,
        backtrace
    ))]
    InitClient {
        full_user_name: String,
        source: obkv::error::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to execute sql, err:{}, sql:{}.\nBacktrace:\n{}",
        source,
        sql,
        backtrace
    ))]
    ExecuteSql {
        sql: String,
        source: obkv::error::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to check table existence, table:{}, err:{}.\nBacktrace:\n{}",
        table_name,
        source,
        backtrace
    ))]
    CheckTable {
        table_name: String,
        source: obkv::error::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Table not created, table:{}.\nBacktrace:\n{}", table_name, backtrace))]
    TableNotCreated {
        table_name: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Table not dropped, table:{}.\nBacktrace:\n{}", table_name, backtrace))]
    TableNotDropped {
        table_name: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to write to table, table:{}, err:{}.\nBacktrace:\n{}",
        table_name,
        source,
        backtrace
    ))]
    WriteTable {
        table_name: String,
        source: obkv::error::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to iter result set, table:{}, err:{}.\nBacktrace:\n{}",
        table_name,
        source,
        backtrace
    ))]
    IterResultSet {
        table_name: String,
        source: obkv::error::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Column not found in result, table:{}, column:{}.\nBacktrace:\n{}",
        table_name,
        column_name,
        backtrace
    ))]
    MissColumn {
        table_name: String,
        column_name: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to execute query, table:{}, err:{}.\nBacktrace:\n{}",
        table_name,
        source,
        backtrace
    ))]
    ExecuteQuery {
        table_name: String,
        source: obkv::error::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to truncate table, table:{}, err:{}.\nBacktrace:\n{}",
        table_name,
        source,
        backtrace
    ))]
    TruncateTable {
        table_name: String,
        source: obkv::error::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to get value from table, table:{}, err:{}.\nBacktrace:\n{}",
        table_name,
        source,
        backtrace
    ))]
    GetValue {
        table_name: String,
        source: obkv::error::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to delete data from table, table:{}, err:{}.\nBacktrace:\n{}",
        table_name,
        source,
        backtrace
    ))]
    DeleteData {
        table_name: String,
        source: obkv::error::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Invalid result num, table:{}, expect:{}, actual:{}.\nBacktrace:\n{}",
        table_name,
        expect,
        actual,
        backtrace
    ))]
    UnexpectedResultNum {
        table_name: String,
        expect: usize,
        actual: usize,
        backtrace: Backtrace,
    },
}

define_result!(Error);

impl Error {
    fn obkv_result_code(&self) -> Option<obkv::ResultCodes> {
        if let Some(obkv::error::Error::Common(obkv::error::CommonErrCode::ObException(code), _)) =
            self.source()
                .and_then(|s| s.downcast_ref::<obkv::error::Error>())
        {
            Some(*code)
        } else {
            None
        }
    }
}

impl TableError for Error {
    fn is_primary_key_duplicate(&self) -> bool {
        self.obkv_result_code().map_or(false, |code| {
            code == obkv::ResultCodes::OB_ERR_PRIMARY_KEY_DUPLICATE
        })
    }
}

const KEY_COLUMN_NAME: &str = "k";
const VALUE_COLUMN_NAME: &str = "v";
const KEY_COLUMN_LEN: usize = 2048;
const VALUE_COLUMN_TYPE: &str = "LONGBLOB";

#[inline]
fn bytes_to_values(bs: &[u8]) -> Vec<Value> {
    vec![Value::from(bs)]
}

/// Batch operations to write to obkv.
pub struct ObkvWriteBatch {
    batch_op: ObTableBatchOperation,
    op_num: usize,
}

impl WriteBatch for ObkvWriteBatch {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            batch_op: ObTableBatchOperation::with_ops_num_raw(capacity),
            op_num: 0,
        }
    }

    fn insert(&mut self, key: &[u8], value: &[u8]) {
        self.batch_op.insert(
            bytes_to_values(key),
            vec![VALUE_COLUMN_NAME.to_string()],
            bytes_to_values(value),
        );
        self.op_num += 1;
    }

    fn insert_or_update(&mut self, key: &[u8], value: &[u8]) {
        self.batch_op.insert_or_update(
            bytes_to_values(key),
            vec![VALUE_COLUMN_NAME.to_string()],
            bytes_to_values(value),
        );
        self.op_num += 1;
    }

    fn delete(&mut self, key: &[u8]) {
        self.batch_op.delete(bytes_to_values(key));
        self.op_num += 1;
    }
}

impl Default for ObkvWriteBatch {
    fn default() -> ObkvWriteBatch {
        Self {
            batch_op: ObTableBatchOperation::raw(),
            op_num: 0,
        }
    }
}

impl From<&SeekKey> for Value {
    fn from(key: &SeekKey) -> Value {
        Value::from(&key.0)
    }
}

// Returns (key, equals).
fn to_scan_range(bound: &KeyBoundary) -> (Vec<Value>, bool) {
    match bound {
        KeyBoundary::Included(v) => (vec![v.into()], true),
        KeyBoundary::Excluded(v) => (vec![v.into()], false),
        KeyBoundary::MinIncluded => (vec![Value::get_min()], true),
        KeyBoundary::MaxIncluded => (vec![Value::get_max()], true),
    }
}

/// Table kv implementation based on obkv.
#[derive(Clone)]
pub struct ObkvImpl {
    client: ObTableClient,

    // The following are configs, if there are too many configs, maybe we should put them
    // on heap to avoid the `ObkvImpl` struct allocating too much stack size.
    enable_purge_recyclebin: bool,
    check_batch_result_num: bool,
    max_create_table_retries: usize,
}

impl fmt::Debug for ObkvImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObkvImpl")
            .field("client", &"ObTableClient".to_string())
            .field("enable_purge_recyclebin", &self.enable_purge_recyclebin)
            .field("check_batch_result_num", &self.check_batch_result_num)
            .field("max_create_table_retries", &self.max_create_table_retries)
            .finish()
    }
}

impl ObkvImpl {
    /// Create a new obkv client instance with given `config`.
    pub fn new(config: ObkvConfig) -> Result<Self> {
        ensure!(config.valid(), InvalidConfig);

        info!(
            "Try to create obkv client, param_url:{}, full_user_name:{}",
            config.param_url, config.full_user_name
        );

        let client = Builder::new()
            .full_user_name(&config.full_user_name)
            .param_url(&config.param_url)
            .running_mode(RunningMode::Normal)
            .password(&config.password)
            .config(config.client.clone().into())
            .build()
            .context(BuildClient {
                full_user_name: &config.full_user_name,
            })?;

        client.init().context(InitClient {
            full_user_name: &config.full_user_name,
        })?;

        info!(
            "Obkv client created, param_url:{}, full_user_name:{}",
            config.param_url, config.full_user_name
        );

        Ok(Self {
            client,
            enable_purge_recyclebin: config.enable_purge_recyclebin,
            check_batch_result_num: config.check_batch_result_num,
            max_create_table_retries: config.max_create_table_retries,
        })
    }

    fn try_create_kv_table(&self, table_name: &str) -> Result<()> {
        let create_sql = format_create_table_sql(table_name);

        info!(
            "Try to create table, table_name:{}, sql:{}",
            table_name, create_sql
        );

        self.client
            .execute_sql(&create_sql)
            .context(ExecuteSql { sql: &create_sql })?;

        // Table is not exist after created.
        let created = self
            .client
            .check_table_exists(table_name)
            .context(CheckTable { table_name })?;

        ensure!(created, TableNotCreated { table_name });

        info!("Obkv table created, table_name:{}", table_name);

        Ok(())
    }

    fn try_drop_kv_table(&self, table_name: &str) -> Result<()> {
        let drop_sql = format_drop_table_sql(table_name, self.enable_purge_recyclebin);

        info!(
            "Try to drop table, table_name:{}, sql:{}",
            table_name, drop_sql
        );

        self.client
            .execute_sql(&drop_sql)
            .context(ExecuteSql { sql: &drop_sql })?;

        let exists = self
            .client
            .check_table_exists(table_name)
            .context(CheckTable { table_name })?;

        ensure!(!exists, TableNotDropped { table_name });

        Ok(())
    }

    fn check_write_batch_op_results(
        &self,
        table_name: &str,
        results: &[TableOpResult],
        expect_num: usize,
    ) -> Result<()> {
        ensure!(
            !self.check_batch_result_num || results.len() == expect_num,
            UnexpectedResultNum {
                table_name,
                expect: expect_num,
                actual: results.len(),
            }
        );

        Ok(())
    }
}

impl TableKv for ObkvImpl {
    type Error = Error;
    type ScanIter = ObkvScanIter;
    type WriteBatch = ObkvWriteBatch;

    fn table_exists(&self, table_name: &str) -> Result<bool> {
        self.client
            .check_table_exists(table_name)
            .context(CheckTable { table_name })
    }

    fn create_table(&self, table_name: &str) -> Result<()> {
        let mut retry = 0;
        loop {
            match self.try_create_kv_table(table_name) {
                Ok(()) => {
                    info!(
                        "Obkv table created, table_name:{}, retry:{}",
                        table_name, retry
                    );

                    return Ok(());
                }
                Err(e) => {
                    error!(
                        "Failed to create table, table_name:{}, retry:{}, err:{}",
                        table_name, retry, e
                    );

                    retry += 1;
                    if retry > self.max_create_table_retries {
                        return Err(e);
                    }
                }
            }
        }
    }

    fn drop_table(&self, table_name: &str) -> Result<()> {
        // Drop table won't retry on failure now.
        self.try_drop_kv_table(table_name).map_err(|e| {
            error!("Failed to drop table, table_name:{}, err:{}", table_name, e);
            e
        })
    }

    fn truncate_table(&self, table_name: &str) -> Result<()> {
        info!("Try to truncate table, table_name:{}", table_name);

        self.client
            .truncate_table(table_name)
            .context(TruncateTable { table_name })
    }

    fn write(
        &self,
        _ctx: WriteContext,
        table_name: &str,
        write_batch: ObkvWriteBatch,
    ) -> Result<()> {
        let results = self
            .client
            .execute_batch(table_name, write_batch.batch_op)
            .context(WriteTable { table_name })?;

        self.check_write_batch_op_results(table_name, &results, write_batch.op_num)?;

        Ok(())
    }

    fn scan(
        &self,
        ctx: ScanContext,
        table_name: &str,
        request: ScanRequest,
    ) -> Result<ObkvScanIter> {
        let iter = ObkvScanIter::new(self.client.clone(), ctx, table_name.to_string(), request)?;

        Ok(iter)
    }

    fn get(&self, table_name: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut values = self
            .client
            .get(
                table_name,
                bytes_to_values(key),
                vec![VALUE_COLUMN_NAME.to_string()],
            )
            .context(GetValue { table_name })?;

        Ok(values.remove(VALUE_COLUMN_NAME).map(Value::as_bytes))
    }

    fn delete(&self, table_name: &str, key: &[u8]) -> std::result::Result<i64, Self::Error> {
        let mut size = self
            .client
            .delete(table_name, bytes_to_values(key))
            .context(DeleteData { table_name })?;

        Ok(size)
    }
}

pub struct ObkvScanIter {
    client: ObTableClient,
    ctx: ScanContext,
    table_name: String,
    request: ScanRequest,

    /// Current result set.
    result_set: Option<QueryResultSet>,
    current_key: Vec<u8>,
    current_value: Vec<u8>,
    result_set_fetched_num: i32,
    /// The iterator has been exhausted.
    eof: bool,
}

impl fmt::Debug for ObkvScanIter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObkvScanIter")
            .field("client", &"ObTableClient".to_string())
            .field("ctx", &self.ctx)
            .field("table_name", &self.table_name)
            .field("request", &self.request)
            .field("result_set", &self.result_set)
            .field("current_key", &self.current_key)
            .field("current_value", &self.current_value)
            .field("result_set_fetched_num", &self.result_set_fetched_num)
            .field("eof", &self.eof)
            .finish()
    }
}

impl ObkvScanIter {
    /// Create a new iterator for scan.
    fn new(
        client: ObTableClient,
        ctx: ScanContext,
        table_name: String,
        request: ScanRequest,
    ) -> Result<ObkvScanIter> {
        assert!(ctx.batch_size > 0);

        let mut iter = ObkvScanIter {
            client,
            ctx,
            table_name,
            request,

            result_set: None,
            current_key: Vec::new(),
            current_value: Vec::new(),
            result_set_fetched_num: 0,
            eof: false,
        };

        iter.init()?;

        Ok(iter)
    }

    /// Init the iterator.
    fn init(&mut self) -> Result<()> {
        let start = to_scan_range(&self.request.start);
        let end = to_scan_range(&self.request.end);
        let query = self
            .new_query()
            .add_scan_range(start.0, start.1, end.0, end.1);

        let result_set = query.execute().context(ExecuteQuery {
            table_name: &self.table_name,
        })?;
        self.result_set = Some(result_set);

        if !self.step_result_set()? {
            // Nothing fetched from first result set, mark eof to true.
            self.eof = true;
        }

        Ok(())
    }

    /// Fetch next key-value pair from the `result_set`, store them to
    /// `current_key`, `current_value`, then return Ok(true). If no entry
    /// was fetched, reset `result_set` to None and returns Ok(false).
    fn step_result_set(&mut self) -> Result<bool> {
        if let Some(result_set) = &mut self.result_set {
            if let Some(row) = result_set.next() {
                self.result_set_fetched_num += 1;

                let row = row.context(IterResultSet {
                    table_name: &self.table_name,
                })?;

                let (key, value) = row_to_key_value(&self.table_name, row)?;
                self.current_key = key;
                self.current_value = value;

                return Ok(true);
            } else {
                self.result_set = None;
            }
        }

        Ok(false)
    }

    /// Scan next key range, init `result_set` and store first key-value from
    /// `result_set` to `current_key`, `current_value`. If next key range
    /// has no data, set `eof` to true.
    fn scan_next_key_range(&mut self) -> Result<()> {
        assert!(self.result_set.is_none());

        if self.result_set_fetched_num < self.ctx.batch_size {
            // We have reached eof.
            self.eof = true;
            return Ok(());
        }

        let current_key = bytes_to_values(&self.current_key);
        let result_set = if self.request.reverse {
            let start = to_scan_range(&self.request.start);
            self.new_query()
                .add_scan_range(start.0, start.1, current_key, false)
                .execute()
                .context(ExecuteQuery {
                    table_name: &self.table_name,
                })?
        } else {
            let end = to_scan_range(&self.request.end);
            self.new_query()
                .add_scan_range(current_key, false, end.0, end.1)
                .execute()
                .context(ExecuteQuery {
                    table_name: &self.table_name,
                })?
        };
        self.result_set = Some(result_set);
        self.result_set_fetched_num = 0;

        if !self.step_result_set()? {
            // No data in result set of next key range.
            self.eof = true;
        }

        Ok(())
    }

    fn new_query(&self) -> impl TableQuery {
        self.client
            .query(&self.table_name)
            .batch_size(self.ctx.batch_size)
            // NOTE: keep the limit same as the batch size so as to avoid stream query session kept
            // on ObServer.
            .limit(None, self.ctx.batch_size)
            .primary_index()
            .select(vec![
                KEY_COLUMN_NAME.to_string(),
                VALUE_COLUMN_NAME.to_string(),
            ])
            .operation_timeout(self.ctx.timeout)
            // Scan order takes `forward` as input, reverse means NOT forward.
            .scan_order(!self.request.reverse)
    }
}

impl ScanIter for ObkvScanIter {
    type Error = Error;

    fn valid(&self) -> bool {
        !self.eof
    }

    fn next(&mut self) -> Result<bool> {
        assert!(self.valid());

        // Try to fetch next key-value from current result set.
        if self.step_result_set()? {
            return Ok(true);
        }

        // Need to scan next key range.
        self.scan_next_key_range()?;

        Ok(self.valid())
    }

    fn key(&self) -> &[u8] {
        assert!(self.valid());

        &self.current_key
    }

    fn value(&self) -> &[u8] {
        assert!(self.valid());

        &self.current_value
    }
}

fn row_to_key_value(
    table_name: &str,
    mut row: HashMap<String, Value>,
) -> Result<(Vec<u8>, Vec<u8>)> {
    let key = row.remove(KEY_COLUMN_NAME).context(MissColumn {
        table_name,
        column_name: KEY_COLUMN_NAME,
    })?;

    let value = row.remove(VALUE_COLUMN_NAME).context(MissColumn {
        table_name,
        column_name: VALUE_COLUMN_NAME,
    })?;

    Ok((key.as_bytes(), value.as_bytes()))
}

fn format_create_table_sql(table_name: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {table_name}(
            {KEY_COLUMN_NAME} VARBINARY({KEY_COLUMN_LEN}),
            {VALUE_COLUMN_NAME} {VALUE_COLUMN_TYPE} NOT NULL,
            PRIMARY KEY({KEY_COLUMN_NAME})
        );"
    )
}

fn format_drop_table_sql(table_name: &str, purge_recyclebin: bool) -> String {
    if purge_recyclebin {
        format!("DROP TABLE IF EXISTS {table_name}; PURGE RECYCLEBIN;")
    } else {
        format!("DROP TABLE IF EXISTS {table_name};")
    }
}
