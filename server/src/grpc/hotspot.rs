// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! hotspot recorder
use std::{fmt::Write, sync::Arc, thread, time::Duration};

use ceresdbproto::storage::{
    PrometheusQueryRequest, RequestContext, RouteRequest, SqlQueryRequest, WriteRequest,
};
use crossbeam::{bounded, Sender};
use log::{info, warn};
use scheduled_thread_pool::ScheduledThreadPool;
use serde::{Deserialize, Serialize};
pub use spin::Mutex as SpinMutex;

use crate::grpc::hotspot_lru::HotspotLru;

type ReadKey = String;
type WriteKey = String;
const TAG: &str = "hotspot autodump";
const RECODER_CHANNEL_CAP: usize = 64 * 1024;

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
            dump_interval: Duration::from_secs(1),
            auto_dump: true,
            auto_dump_len: 10,
        }
    }
}

enum Message {
    // (ReadKey)
    Read(ReadKey),
    // (WriteKey, field_num)
    Write(WriteKey, usize),
}

#[derive(Clone)]
pub struct HotspotRecorder {
    tx: Arc<Sender<Message>>,
    hotspot_read: Option<Arc<SpinMutex<HotspotLru<ReadKey>>>>,
    hotspot_write: Option<Arc<SpinMutex<HotspotLru<WriteKey>>>>,
    dump_pool: Option<Arc<ScheduledThreadPool>>,
}

#[derive(Clone)]
pub struct Dump {
    pub read_hots: Vec<String>,
    pub write_hots: Vec<String>,
}

impl HotspotRecorder {
    pub fn new(config: Config) -> Self {
        let hotspot_read = config
            .read_cap
            .map(|cap| Arc::new(SpinMutex::new(HotspotLru::new(cap))));

        let hotspot_write = config
            .write_cap
            .map(|cap| Arc::new(SpinMutex::new(HotspotLru::new(cap))));

        let (hr, hw) = (hotspot_read.clone(), hotspot_write.clone());
        let (tx, rx) = bounded(RECODER_CHANNEL_CAP);
        thread::Builder::new()
            .name("hotspot-recoder".to_owned())
            .spawn(move || loop {
                match rx.recv() {
                    Err(_) => {
                        warn!("Hotspot recoder sender stopped");
                        break;
                    }
                    Ok(msg) => match msg {
                        Message::Read(read_key) => {
                            if let Some(hotspot) = &hr {
                                hotspot.lock().inc(&read_key, 1);
                            }
                        }
                        Message::Write(write_key, field_num) => {
                            if let Some(hotspot) = &hw {
                                hotspot.lock().inc(&write_key, field_num as u64);
                            }
                        }
                    },
                }
            })
            .expect("Fail to create hotspot-recoder thread");

        let dump_pool = if config.auto_dump {
            Some(Arc::new(ScheduledThreadPool::with_name("hotspot_dump", 1)))
        } else {
            None
        };

        let recorder = Self {
            tx: Arc::new(tx),
            hotspot_read,
            hotspot_write,
            dump_pool,
        };

        if let Some(pool) = &recorder.dump_pool {
            let recoder = recorder.clone();
            let interval = config.dump_interval;
            let dump_len = config.auto_dump_len;
            pool.execute_at_fixed_rate(interval * 2, interval, move || {
                let Dump {
                    read_hots,
                    write_hots,
                } = recoder.dump();
                read_hots
                    .into_iter()
                    .take(dump_len)
                    .for_each(|hot| info!("{} read {}", TAG, hot));
                write_hots
                    .into_iter()
                    .take(dump_len)
                    .for_each(|hot| info!("{} write {}", TAG, hot));
            });
        };

        recorder
    }

    fn key_prefix(context: &RequestContext) -> String {
        let mut prefix = String::new();

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
                    "inc_read_reqs",
                    Message::Read(Self::table_hot_key(&req.context.clone().unwrap(), table)),
                );
            }
        }
    }

    pub fn inc_write_reqs(&self, req: &WriteRequest) {
        if self.hotspot_write.is_some() {
            for table_request in &req.table_requests {
                let hot_key =
                    Self::table_hot_key(&req.context.clone().unwrap(), &table_request.table);
                let mut field_num = 0;
                for entry in &table_request.entries {
                    for field_group in &entry.field_groups {
                        field_num += field_group.fields.len();
                    }
                }
                self.send_msg_or_log("inc_write_reqs", Message::Write(hot_key, field_num));
            }
        }
    }

    pub fn inc_route_reqs(&self, _req: &RouteRequest) {}

    pub fn inc_prom_query_reqs(&self, _req: &PrometheusQueryRequest) {}

    fn send_msg_or_log(&self, method: &str, msg: Message) {
        if let Err(e) = self.tx.clone().try_send(msg) {
            warn!(
                "HotspotRecoder::{} fail to send \
                measurement to recoder, err:{}",
                method, e
            );
        }
    }

    fn pop_read_hots(&self) -> Option<Vec<(ReadKey, u64)>> {
        HotspotRecorder::pop_hots(&self.hotspot_read)
    }

    fn pop_write_hots(&self) -> Option<Vec<(WriteKey, u64)>> {
        HotspotRecorder::pop_hots(&self.hotspot_write)
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

    // dump return read / fields read / write metrics / search_series
    pub fn dump(&self) -> Dump {
        let format_hots = |hots: Vec<(String, u64)>| {
            hots.into_iter()
                .map(|(k, v)| format!("metric={k}, heats={v}"))
                .collect()
        };

        Dump {
            read_hots: self.pop_read_hots().map_or_else(Vec::new, format_hots),
            write_hots: self.pop_write_hots().map_or_else(Vec::new, format_hots),
        }
    }
}

#[cfg(test)]
mod test {
    use std::{thread, time};

    use ceresdbproto::{
        storage,
        storage::{
            value::Value::StringValue, Field, FieldGroup, Value, WriteSeriesEntry,
            WriteTableRequest,
        },
    };

    use super::*;

    #[test]
    fn test_hotspot() {
        let read_cap: Option<usize> = Some(3);
        let write_cap: Option<usize> = Some(3);
        let options = Config {
            read_cap,
            write_cap,
            auto_dump: false,
            dump_interval: time::Duration::from_millis(1000),
            auto_dump_len: 10,
        };

        let hotspot = HotspotRecorder::new(options);

        assert!(hotspot.pop_read_hots().unwrap().is_empty());
        assert!(hotspot.pop_write_hots().unwrap().is_empty());
        let table = String::from("table1");
        let context = mock_context();
        let req = SqlQueryRequest {
            context,
            tables: vec![table],
            sql: String::from("select * from table1 limit 10"),
        };

        hotspot.inc_sql_query_reqs(&req);

        thread::sleep(time::Duration::from_millis(100u64));
        let vec = hotspot.pop_read_hots().unwrap();

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
            dump_interval: time::Duration::from_millis(1000),
            auto_dump_len: 10,
        };

        let hotspot = HotspotRecorder::new(options);

        assert!(hotspot.pop_read_hots().unwrap().is_empty());
        assert!(hotspot.pop_write_hots().unwrap().is_empty());

        let table = String::from("table1");
        let context = mock_context();
        let query_req = SqlQueryRequest {
            context,
            tables: vec![table.clone()],
            sql: String::from("select * from table1 limit 10"),
        };

        hotspot.inc_sql_query_reqs(&query_req);
        let write_req = WriteRequest {
            context: mock_context(),
            table_requests: vec![WriteTableRequest {
                table,
                tag_names: vec![String::from("name")],
                field_names: vec![String::from("value")],
                entries: vec![WriteSeriesEntry {
                    tags: vec![storage::Tag {
                        name_index: 0,
                        value: Some(Value {
                            value: Some(StringValue(String::from("name1"))),
                        }),
                    }],
                    field_groups: vec![FieldGroup {
                        timestamp: 1679647020000,
                        fields: vec![Field {
                            name_index: 0,
                            value: Some(Value { value: None }),
                        }],
                    }],
                }],
            }],
        };
        hotspot.inc_write_reqs(&write_req);

        thread::sleep(time::Duration::from_millis(100u64));

        let Dump {
            read_hots,
            write_hots,
        } = hotspot.dump();
        assert_eq!(vec!["metric=public/table1, heats=1"], read_hots);
        assert_eq!(vec!["metric=public/table1, heats=1",], write_hots);
    }

    fn mock_context() -> Option<RequestContext> {
        Some(RequestContext {
            database: String::from("public"),
        })
    }
}
