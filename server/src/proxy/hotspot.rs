// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! hotspot recorder
use std::{fmt::Write, sync::Arc, time::Duration};

use ceresdbproto::storage::{
    PrometheusQueryRequest, RequestContext, SqlQueryRequest, WriteRequest,
};
use common_util::{runtime::Runtime, timed_task::TimedTask};
use log::{info, warn};
use serde::{Deserialize, Serialize};
pub use spin::Mutex as SpinMutex;
use tokio::sync::mpsc::{self, UnboundedSender};

use crate::proxy::{hotspot_lru::HotspotLru, util};

type ReadKey = String;
type WriteKey = String;
const TAG: &str = "hotspot autodump";

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct Config {
    // Max items size for read hotspot
    read_cap: Option<usize>,
    // Max items size for write hotspot
    write_cap: Option<usize>,
    dump_interval: Duration,
    auto_dump: bool,
    // Max items for dump hotspot
    auto_dump_len: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            read_cap: Some(10_000),
            write_cap: Some(10_000),
            dump_interval: Duration::from_secs(5),
            auto_dump: true,
            auto_dump_len: 10,
        }
    }
}

enum Message {
    // (ReadKey)
    Query(ReadKey),
    // (WriteKey, row_count, field_count)
    Write(WriteKey, usize, usize),
}

#[derive(Clone)]
pub struct HotspotRecorder {
    tx: Arc<UnboundedSender<Message>>,
    hotspot_read: Option<Arc<SpinMutex<HotspotLru<ReadKey>>>>,
    hotspot_write: Option<Arc<SpinMutex<HotspotLru<WriteKey>>>>,
    hotspot_field_write: Option<Arc<SpinMutex<HotspotLru<WriteKey>>>>,
}

#[derive(Clone)]
pub struct Dump {
    pub read_hots: Vec<String>,
    pub write_hots: Vec<String>,
    pub write_field_hots: Vec<String>,
}

impl HotspotRecorder {
    pub fn new(config: Config, runtime: Arc<Runtime>) -> Self {
        let hotspot_read = config
            .read_cap
            .map(|cap| Arc::new(SpinMutex::new(HotspotLru::new(cap))));

        let hotspot_write = config
            .write_cap
            .map(|cap| Arc::new(SpinMutex::new(HotspotLru::new(cap))));

        let hotspot_field_write = config
            .write_cap
            .map(|cap| Arc::new(SpinMutex::new(HotspotLru::new(cap))));

        let (hr, hw, hwf) = (
            hotspot_read.clone(),
            hotspot_write.clone(),
            hotspot_field_write.clone(),
        );

        let (tx, mut rx) = mpsc::unbounded_channel();
        let recorder = Self {
            tx: Arc::new(tx),
            hotspot_read,
            hotspot_write,
            hotspot_field_write,
        };

        let task_handle = if config.auto_dump {
            let interval = config.dump_interval;
            let dump_len = config.auto_dump_len;
            let recorder_clone = recorder.clone();
            let builder = move || {
                let recorder_in_builder = recorder_clone.clone();
                async move {
                    let Dump {
                        read_hots,
                        write_hots,
                        write_field_hots,
                    } = recorder_in_builder.dump();

                    read_hots
                        .into_iter()
                        .take(dump_len)
                        .for_each(|hot| info!("{} read {}", TAG, hot));
                    write_hots
                        .into_iter()
                        .take(dump_len)
                        .for_each(|hot| info!("{} write rows {}", TAG, hot));
                    write_field_hots
                        .into_iter()
                        .take(dump_len)
                        .for_each(|hot| info!("{} write fields {}", TAG, hot));
                }
            };

            Some(TimedTask::start_timed_task(
                String::from("hotspot_dump"),
                &runtime,
                interval,
                builder,
            ))
        } else {
            None
        };

        runtime.spawn(async move {
            loop {
                match rx.recv().await {
                    None => {
                        warn!("Hotspot recoder sender stopped");
                        if let Some(handle) = task_handle {
                            handle.stop_task().await.unwrap();
                        }
                        break;
                    }
                    Some(msg) => match msg {
                        Message::Query(read_key) => {
                            if let Some(hotspot) = &hr {
                                hotspot.lock().inc(&read_key, 1);
                            }
                        }
                        Message::Write(write_key, row_count, field_count) => {
                            if let Some(hotspot) = &hw {
                                hotspot.lock().inc(&write_key, row_count as u64);
                            }

                            if let Some(hotspot) = &hwf {
                                hotspot.lock().inc(&write_key, field_count as u64);
                            }
                        }
                    },
                }
            }
        });

        recorder
    }

    fn key_prefix(context: &RequestContext) -> String {
        let mut prefix = String::new();

        // use database as prefix
        if !context.database.is_empty() {
            write!(prefix, "{}/", context.database).unwrap();
        }

        prefix
    }

    fn table_hot_key(context: &RequestContext, table: &String) -> String {
        let prefix = Self::key_prefix(context);
        prefix + table
    }

    pub fn inc_sql_query_reqs(&self, req: &SqlQueryRequest) {
        if self.hotspot_read.is_some() {
            for table in &req.tables {
                self.send_msg_or_log(
                    "inc_query_reqs",
                    Message::Query(Self::table_hot_key(&req.context.clone().unwrap(), table)),
                );
            }
        }
    }

    pub fn inc_write_reqs(&self, req: &WriteRequest) {
        if self.hotspot_write.is_some() && self.hotspot_field_write.is_some() {
            for table_request in &req.table_requests {
                let hot_key =
                    Self::table_hot_key(&req.context.clone().unwrap(), &table_request.table);
                let mut row_count = 0;
                let mut field_count = 0;
                for entry in &table_request.entries {
                    row_count += 1;
                    for field_group in &entry.field_groups {
                        field_count += field_group.fields.len();
                    }
                }
                self.send_msg_or_log(
                    "inc_write_reqs",
                    Message::Write(hot_key, row_count, field_count),
                );
            }
        }
    }

    pub fn inc_promql_reqs(&self, req: &PrometheusQueryRequest) {
        if self.hotspot_read.is_some() {
            if let Some(expr) = &req.expr {
                if let Some(table) = util::table_from_expr(expr) {
                    let hot_key = Self::table_hot_key(&req.context.clone().unwrap(), &table);
                    self.send_msg_or_log("inc_query_reqs", Message::Query(hot_key))
                }
            }
        }
    }

    /// return read count / write row count / write field count
    pub fn dump(&self) -> Dump {
        let format_hots = |hots: Vec<(String, u64)>| {
            hots.into_iter()
                .map(|(k, v)| format!("metric={k}, heats={v}"))
                .collect()
        };

        Dump {
            read_hots: self.pop_read_hots().map_or_else(Vec::new, format_hots),
            write_hots: self.pop_write_hots().map_or_else(Vec::new, format_hots),
            write_field_hots: self
                .pop_write_field_hots()
                .map_or_else(Vec::new, format_hots),
        }
    }

    fn pop_read_hots(&self) -> Option<Vec<(ReadKey, u64)>> {
        HotspotRecorder::pop_hots(&self.hotspot_read)
    }

    fn pop_write_hots(&self) -> Option<Vec<(WriteKey, u64)>> {
        HotspotRecorder::pop_hots(&self.hotspot_write)
    }

    fn pop_write_field_hots(&self) -> Option<Vec<(WriteKey, u64)>> {
        HotspotRecorder::pop_hots(&self.hotspot_field_write)
    }

    fn pop_hots(target: &Option<Arc<SpinMutex<HotspotLru<String>>>>) -> Option<Vec<(String, u64)>> {
        match target {
            Some(hotspot) => {
                let mut hots = hotspot.lock().pop_all();
                hots.sort_by(|a, b| b.1.cmp(&a.1));

                Some(hots)
            }
            _ => None,
        }
    }

    fn send_msg_or_log(&self, method: &str, msg: Message) {
        if let Err(e) = self.tx.clone().send(msg) {
            warn!(
                "HotspotRecoder::{} fail to send \
                measurement to recoder, err:{}",
                method, e
            );
        }
    }
}

#[cfg(test)]
mod test {
    use std::thread;

    use ceresdbproto::{
        storage,
        storage::{
            value::Value::StringValue, Field, FieldGroup, Value, WriteSeriesEntry,
            WriteTableRequest,
        },
    };
    use common_util::runtime::Builder;

    fn new_runtime() -> Arc<Runtime> {
        let runtime = Builder::default()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();

        Arc::new(runtime)
    }

    use super::*;

    #[test]
    fn test_hotspot() {
        let read_cap: Option<usize> = Some(3);
        let write_cap: Option<usize> = Some(3);
        let options = Config {
            read_cap,
            write_cap,
            auto_dump: false,
            dump_interval: Duration::from_millis(5000),
            auto_dump_len: 10,
        };

        let recorder = HotspotRecorder::new(options, new_runtime());
        assert!(recorder.pop_read_hots().unwrap().is_empty());
        assert!(recorder.pop_write_hots().unwrap().is_empty());
        let table = String::from("table1");
        let context = mock_context();
        let req = SqlQueryRequest {
            context,
            tables: vec![table],
            sql: String::from("select * from table1 limit 10"),
        };

        recorder.inc_sql_query_reqs(&req);

        let vec = recorder.pop_read_hots().unwrap();
        assert_eq!(1, vec.len());
        assert_eq!("public/table1", vec.get(0).unwrap().0);
    }

    #[test]
    fn test_hotspot_dump() {
        let read_cap: Option<usize> = Some(10);
        let write_cap: Option<usize> = Some(10);
        let options = Config {
            read_cap,
            write_cap,
            auto_dump: false,
            dump_interval: Duration::from_millis(5000),
            auto_dump_len: 10,
        };

        let recorder = HotspotRecorder::new(options, new_runtime());

        assert!(recorder.pop_read_hots().unwrap().is_empty());
        assert!(recorder.pop_write_hots().unwrap().is_empty());

        let table = String::from("table1");
        let context = mock_context();
        let query_req = SqlQueryRequest {
            context,
            tables: vec![table.clone()],
            sql: String::from("select * from table1 limit 10"),
        };
        recorder.inc_sql_query_reqs(&query_req);

        let write_req = WriteRequest {
            context: mock_context(),
            table_requests: vec![WriteTableRequest {
                table,
                tag_names: vec![String::from("name")],
                field_names: vec![String::from("value1"), String::from("value2")],
                entries: vec![WriteSeriesEntry {
                    tags: vec![storage::Tag {
                        name_index: 0,
                        value: Some(Value {
                            value: Some(StringValue(String::from("name1"))),
                        }),
                    }],
                    field_groups: vec![FieldGroup {
                        timestamp: 1679647020000,
                        fields: vec![
                            Field {
                                name_index: 0,
                                value: Some(Value { value: None }),
                            },
                            Field {
                                name_index: 1,
                                value: Some(Value { value: None }),
                            },
                        ],
                    }],
                }],
            }],
        };
        recorder.inc_write_reqs(&write_req);

        thread::sleep(Duration::from_millis(100));
        let Dump {
            read_hots,
            write_hots,
            write_field_hots,
        } = recorder.dump();
        assert_eq!(vec!["metric=public/table1, heats=1",], write_hots);
        assert_eq!(vec!["metric=public/table1, heats=1"], read_hots);
        assert_eq!(vec!["metric=public/table1, heats=2",], write_field_hots);
    }

    fn mock_context() -> Option<RequestContext> {
        Some(RequestContext {
            database: String::from("public"),
        })
    }
}
