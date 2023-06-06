// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Config of table kv.

use common_util::config::ReadableDuration;
use serde::{Deserialize, Serialize};

// TODO: use test conf to control which environments to test.
const TEST_FULL_USER_NAME: &str = "user_name";
const TEST_URL: &str = "url";
const TEST_PASSWORD: &str = "passwd";
const TEST_SYS_USER_NAME: &str = "";
const TEST_SYS_PASSWORD: &str = "";

/// Config of obkv.
#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(default)]
pub struct ObkvConfig {
    pub full_user_name: String,
    pub param_url: String,
    pub password: String,
    pub check_batch_result_num: bool,
    pub enable_purge_recyclebin: bool,
    pub max_create_table_retries: usize,
    pub create_table_retry_interval: ReadableDuration,
    pub client: ClientConfig,
}

impl Default for ObkvConfig {
    fn default() -> Self {
        Self {
            full_user_name: String::new(),
            param_url: String::new(),
            password: String::new(),
            check_batch_result_num: true,
            enable_purge_recyclebin: false,
            max_create_table_retries: 2,
            create_table_retry_interval: ReadableDuration::secs(5),
            client: ClientConfig::default(),
        }
    }
}

impl ObkvConfig {
    pub fn valid(&self) -> bool {
        !self.full_user_name.is_empty() && !self.param_url.is_empty()
    }

    /// Create a test-only obkv config.
    pub fn for_test() -> Self {
        let mut config = ObkvConfig {
            full_user_name: TEST_FULL_USER_NAME.to_string(),
            param_url: TEST_URL.to_string(),
            password: TEST_PASSWORD.to_string(),
            ..Default::default()
        };
        config.client.metadata_mysql_conn_pool_min_size = 1;
        config.client.metadata_mysql_conn_pool_max_size = 1;
        config.client.sys_user_name = TEST_SYS_USER_NAME.to_string();
        config.client.sys_password = TEST_SYS_PASSWORD.to_string();

        config
    }
}

/// Obkv server log level.
#[derive(Copy, Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum ObLogLevel {
    #[default]
    None = 7,
    Error = 0,
    Warn = 2,
    Info = 3,
    Trace = 4,
    Debug = 5,
}

impl From<u16> for ObLogLevel {
    fn from(level: u16) -> Self {
        match level {
            level if level == ObLogLevel::None as u16 => ObLogLevel::None,
            level if level == ObLogLevel::Error as u16 => ObLogLevel::Error,
            level if level == ObLogLevel::Warn as u16 => ObLogLevel::Warn,
            level if level == ObLogLevel::Info as u16 => ObLogLevel::Info,
            level if level == ObLogLevel::Trace as u16 => ObLogLevel::Trace,
            level if level == ObLogLevel::Debug as u16 => ObLogLevel::Debug,
            _ => ObLogLevel::None,
        }
    }
}

/// Config of obkv client.
#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(default)]
pub struct ClientConfig {
    pub sys_user_name: String,
    pub sys_password: String,
    pub metadata_refresh_interval: ReadableDuration,
    pub ocp_model_cache_file: String,
    pub rslist_acquire_timeout: ReadableDuration,
    pub rslist_acquire_try_times: usize,
    pub rslist_acquire_retry_interval: ReadableDuration,
    pub table_entry_acquire_connect_timeout: ReadableDuration,
    pub table_entry_acquire_read_timeout: ReadableDuration,
    pub table_entry_refresh_interval_base: ReadableDuration,
    pub table_entry_refresh_interval_ceiling: ReadableDuration,
    pub table_entry_refresh_try_times: usize,
    pub table_entry_refresh_try_interval: ReadableDuration,
    pub table_entry_refresh_continuous_failure_ceiling: usize,
    pub server_address_priority_timeout: ReadableDuration,
    pub runtime_continuous_failure_ceiling: usize,
    pub rpc_connect_timeout: ReadableDuration,
    pub rpc_read_timeout: ReadableDuration,
    pub rpc_operation_timeout: ReadableDuration,
    pub rpc_login_timeout: ReadableDuration,
    pub rpc_retry_limit: usize,
    pub rpc_retry_interval: ReadableDuration,
    pub refresh_workers_num: usize,
    pub max_conns_per_server: usize,
    pub min_idle_conns_per_server: usize,
    pub conn_init_thread_num: usize,
    pub metadata_mysql_conn_pool_max_size: usize,
    pub metadata_mysql_conn_pool_min_size: usize,
    pub table_batch_op_thread_num: usize,
    pub query_concurrency_limit: Option<usize>,
    pub log_level_flag: ObLogLevel,
}

impl From<obkv::ClientConfig> for ClientConfig {
    fn from(client_config: obkv::ClientConfig) -> Self {
        Self {
            sys_user_name: client_config.sys_user_name,
            sys_password: client_config.sys_password,
            metadata_refresh_interval: client_config.metadata_refresh_interval.into(),
            ocp_model_cache_file: client_config.ocp_model_cache_file,
            rslist_acquire_timeout: client_config.rslist_acquire_timeout.into(),
            rslist_acquire_try_times: client_config.rslist_acquire_try_times,
            rslist_acquire_retry_interval: client_config.rslist_acquire_retry_interval.into(),
            table_entry_acquire_connect_timeout: client_config
                .table_entry_acquire_connect_timeout
                .into(),
            table_entry_acquire_read_timeout: client_config.table_entry_acquire_read_timeout.into(),
            table_entry_refresh_interval_base: client_config
                .table_entry_refresh_interval_base
                .into(),
            table_entry_refresh_interval_ceiling: client_config
                .table_entry_refresh_interval_ceiling
                .into(),
            table_entry_refresh_try_times: client_config.table_entry_refresh_try_times,
            table_entry_refresh_try_interval: client_config.table_entry_refresh_try_interval.into(),
            table_entry_refresh_continuous_failure_ceiling: client_config
                .table_entry_refresh_continuous_failure_ceiling,
            server_address_priority_timeout: client_config.server_address_priority_timeout.into(),
            runtime_continuous_failure_ceiling: client_config.runtime_continuous_failure_ceiling,
            rpc_connect_timeout: client_config.rpc_connect_timeout.into(),
            rpc_read_timeout: client_config.rpc_read_timeout.into(),
            rpc_operation_timeout: client_config.rpc_operation_timeout.into(),
            rpc_login_timeout: client_config.rpc_login_timeout.into(),
            rpc_retry_limit: client_config.rpc_retry_limit,
            rpc_retry_interval: client_config.rpc_retry_interval.into(),
            refresh_workers_num: client_config.refresh_workers_num,
            max_conns_per_server: client_config.max_conns_per_server,
            min_idle_conns_per_server: client_config.min_idle_conns_per_server,
            conn_init_thread_num: client_config.conn_init_thread_num,
            metadata_mysql_conn_pool_max_size: client_config.metadata_mysql_conn_pool_max_size,
            metadata_mysql_conn_pool_min_size: client_config.metadata_mysql_conn_pool_min_size,
            table_batch_op_thread_num: client_config.table_batch_op_thread_num,
            query_concurrency_limit: client_config.query_concurrency_limit,
            log_level_flag: client_config.log_level_flag.into(),
        }
    }
}

impl Default for ClientConfig {
    fn default() -> Self {
        let client_config = obkv::ClientConfig::default();

        Self::from(client_config)
    }
}

impl From<ClientConfig> for obkv::ClientConfig {
    fn from(config: ClientConfig) -> obkv::ClientConfig {
        obkv::ClientConfig {
            sys_user_name: config.sys_user_name,
            sys_password: config.sys_password,
            metadata_refresh_interval: config.metadata_refresh_interval.into(),
            ocp_model_cache_file: config.ocp_model_cache_file,
            rslist_acquire_timeout: config.rslist_acquire_timeout.into(),
            rslist_acquire_try_times: config.rslist_acquire_try_times,
            rslist_acquire_retry_interval: config.rslist_acquire_retry_interval.into(),
            table_entry_acquire_connect_timeout: config.table_entry_acquire_connect_timeout.into(),
            table_entry_acquire_read_timeout: config.table_entry_acquire_read_timeout.into(),
            table_entry_refresh_interval_base: config.table_entry_refresh_interval_base.into(),
            table_entry_refresh_interval_ceiling: config
                .table_entry_refresh_interval_ceiling
                .into(),
            table_entry_refresh_try_times: config.table_entry_refresh_try_times,
            table_entry_refresh_try_interval: config.table_entry_refresh_try_interval.into(),
            table_entry_refresh_continuous_failure_ceiling: config
                .table_entry_refresh_continuous_failure_ceiling,
            server_address_priority_timeout: config.server_address_priority_timeout.into(),
            runtime_continuous_failure_ceiling: config.runtime_continuous_failure_ceiling,
            rpc_connect_timeout: config.rpc_connect_timeout.into(),
            rpc_read_timeout: config.rpc_read_timeout.into(),
            rpc_operation_timeout: config.rpc_operation_timeout.into(),
            rpc_login_timeout: config.rpc_login_timeout.into(),
            rpc_retry_limit: config.rpc_retry_limit,
            rpc_retry_interval: config.rpc_retry_interval.into(),
            refresh_workers_num: config.refresh_workers_num,
            max_conns_per_server: config.max_conns_per_server,
            min_idle_conns_per_server: config.min_idle_conns_per_server,
            conn_init_thread_num: config.conn_init_thread_num,
            metadata_mysql_conn_pool_max_size: config.metadata_mysql_conn_pool_max_size,
            metadata_mysql_conn_pool_min_size: config.metadata_mysql_conn_pool_min_size,
            table_batch_op_thread_num: config.table_batch_op_thread_num,
            query_concurrency_limit: config.query_concurrency_limit,
            log_level_flag: config.log_level_flag as u16,
        }
    }
}
