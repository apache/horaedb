// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Http service

use std::{
    collections::HashMap, convert::Infallible, error::Error as StdError, fs::File, net::IpAddr,
    sync::Arc, thread, time::Duration,
};

use analytic_engine::setup::OpenedWals;
use common_types::bytes::Bytes;
use common_util::{
    error::{BoxError, GenericError},
    runtime::Runtime,
};
use handlers::query::QueryRequest as HandlerQueryRequest;
use log::{error, info};
use logger::RuntimeLevel;
use profile::Profiler;
use prom_remote_api::web;
use query_engine::executor::Executor as QueryExecutor;
use router::{endpoint::Endpoint, RouterRef};
use serde::Serialize;
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use table_engine::{engine::EngineRuntimes, table::FlushRequest};
use tokio::sync::oneshot::{self, Receiver, Sender};
use warp::{
    header,
    http::StatusCode,
    reject,
    reply::{self, Reply},
    Filter,
};

use crate::{
    consts,
    context::RequestContext,
    error_util,
    handlers::{
        self,
        influxdb::{self, InfluxDb, InfluxqlParams, InfluxqlRequest, WriteParams, WriteRequest},
    },
    instance::InstanceRef,
    metrics,
    proxy::{
        http::query::{convert_output, QueryRequest, Request},
        Proxy,
    },
    schema_config_provider::SchemaConfigProviderRef,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to create request context, err:{}", source))]
    CreateContext { source: crate::context::Error },

    #[snafu(display("Failed to handle request, err:{}", source))]
    HandleRequest { source: GenericError },

    #[snafu(display("Failed to handle update log level, err:{}", msg))]
    HandleUpdateLogLevel { msg: String },

    #[snafu(display("Missing engine runtimes to build service.\nBacktrace:\n{}", backtrace))]
    MissingEngineRuntimes { backtrace: Backtrace },

    #[snafu(display("Missing log runtime to build service.\nBacktrace:\n{}", backtrace))]
    MissingLogRuntime { backtrace: Backtrace },

    #[snafu(display("Missing instance to build service.\nBacktrace:\n{}", backtrace))]
    MissingInstance { backtrace: Backtrace },

    #[snafu(display("Missing schema config provider.\nBacktrace:\n{}", backtrace))]
    MissingSchemaConfigProvider { backtrace: Backtrace },

    #[snafu(display("Missing proxy.\nBacktrace:\n{}", backtrace))]
    MissingProxy { backtrace: Backtrace },

    #[snafu(display(
        "Fail to do heap profiling, err:{}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    ProfileHeap {
        source: profile::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Fail to join async task, err:{}.", source))]
    JoinAsyncTask { source: common_util::runtime::Error },

    #[snafu(display(
        "Failed to parse ip addr, ip:{}, err:{}.\nBacktrace:\n{}",
        ip,
        source,
        backtrace
    ))]
    ParseIpAddr {
        ip: String,
        source: std::net::AddrParseError,
        backtrace: Backtrace,
    },

    #[snafu(display("Internal err:{}.", source))]
    Internal {
        source: Box<dyn StdError + Send + Sync>,
    },

    #[snafu(display("Server already started.\nBacktrace:\n{}", backtrace))]
    AlreadyStarted { backtrace: Backtrace },

    #[snafu(display("Missing router.\nBacktrace:\n{}", backtrace))]
    MissingRouter { backtrace: Backtrace },

    #[snafu(display("Missing wal.\nBacktrace:\n{}", backtrace))]
    MissingWal { backtrace: Backtrace },
}

define_result!(Error);

impl reject::Reject for Error {}

pub const DEFAULT_MAX_BODY_SIZE: u64 = 64 * 1024;

/// Http service
///
/// Endpoints beginning with /debug are for internal use, and may subject to
/// breaking changes.
pub struct Service<Q> {
    proxy: Arc<Proxy<Q>>,
    engine_runtimes: Arc<EngineRuntimes>,
    log_runtime: Arc<RuntimeLevel>,
    profiler: Arc<Profiler>,
    influxdb: Arc<InfluxDb<Q>>,
    tx: Sender<()>,
    rx: Option<Receiver<()>>,
    config: HttpConfig,
    config_content: String,
    router: RouterRef,
    opened_wals: OpenedWals,
}

impl<Q: QueryExecutor + 'static> Service<Q> {
    pub async fn start(&mut self) -> Result<()> {
        let ip_addr: IpAddr = self
            .config
            .endpoint
            .addr
            .parse()
            .with_context(|| ParseIpAddr {
                ip: self.config.endpoint.addr.to_string(),
            })?;
        let rx = self.rx.take().context(AlreadyStarted)?;

        info!(
            "HTTP server tries to listen on {}",
            &self.config.endpoint.to_string()
        );

        // Register filters to warp and rejection handler
        let routes = self.routes().recover(handle_rejection);
        let (_addr, server) = warp::serve(routes).bind_with_graceful_shutdown(
            (ip_addr, self.config.endpoint.port),
            async {
                rx.await.ok();
            },
        );

        self.engine_runtimes.default_runtime.spawn(server);

        Ok(())
    }

    pub fn stop(self) {
        if let Err(e) = self.tx.send(()) {
            error!("Failed to send http service stop message, err:{:?}", e);
        }
    }
}

impl<Q: QueryExecutor + 'static> Service<Q> {
    fn routes(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        self.home()
            // public APIs
            .or(self.metrics())
            .or(self.sql())
            .or(self.influxdb_api())
            .or(self.prom_api())
            .or(self.route())
            // admin APIs
            .or(self.admin_block())
            // debug APIs
            .or(self.flush_memtable())
            .or(self.update_log_level())
            .or(self.heap_profile())
            .or(self.cpu_profile())
            .or(self.server_config())
            .or(self.stats())
    }

    /// Expose `/prom/v1/read` and `/prom/v1/write` to serve Prometheus remote
    /// storage request
    fn prom_api(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        let write_api = warp::path!("write")
            .and(web::warp::with_remote_storage(self.proxy.clone()))
            .and(self.with_context())
            .and(web::warp::protobuf_body())
            .and_then(web::warp::write);
        let query_api = warp::path!("read")
            .and(web::warp::with_remote_storage(self.proxy.clone()))
            .and(self.with_context())
            .and(web::warp::protobuf_body())
            .and_then(web::warp::read);

        warp::path!("prom" / "v1" / ..)
            .and(warp::post())
            .and(warp::body::content_length_limit(self.config.max_body_size))
            .and(write_api.or(query_api))
    }

    // GET /
    fn home(&self) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path::end().and(warp::get()).map(|| {
            let mut resp = HashMap::new();
            resp.insert("status", "ok");
            reply::json(&resp)
        })
    }

    // POST /sql
    fn sql(&self) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        // accept json or plain text
        let extract_request = warp::body::json()
            .or(warp::body::bytes().map(|v: Bytes| Request {
                query: String::from_utf8_lossy(&v).to_string(),
            }))
            .unify();

        warp::path!("sql")
            .and(warp::post())
            .and(warp::body::content_length_limit(self.config.max_body_size))
            .and(extract_request)
            .and(self.with_context())
            .and(self.with_proxy())
            .and_then(|req, ctx, proxy: Arc<Proxy<Q>>| async move {
                let req = QueryRequest::Sql(req);
                let result = proxy
                    .handle_query(&ctx, req)
                    .await
                    .map(convert_output)
                    .box_err()
                    .context(HandleRequest);
                match result {
                    Ok(res) => Ok(reply::json(&res)),
                    Err(e) => Err(reject::custom(e)),
                }
            })
    }

    // GET /route
    fn route(&self) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("route" / String)
            .and(warp::get())
            .and(self.with_context())
            .and(self.with_router())
            .and_then(|table: String, ctx, router| async move {
                let result = handlers::route::handle_route(ctx, router, table)
                    .await
                    .box_err()
                    .context(HandleRequest);
                match result {
                    Ok(res) => Ok(reply::json(&res)),
                    Err(e) => Err(reject::custom(e)),
                }
            })
    }

    /// for write api:
    ///     POST `/influxdb/v1/write`
    ///
    /// for query api:
    ///     POST/GET `/influxdb/v1/query`
    ///
    /// It's derived from the influxdb 1.x query api described doc of 1.8:
    ///     https://docs.influxdata.com/influxdb/v1.8/tools/api/#query-http-endpoint
    fn influxdb_api(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        let body_limit = warp::body::content_length_limit(self.config.max_body_size);

        let write_api = warp::path!("write")
            .and(warp::post())
            .and(body_limit)
            .and(self.with_context())
            .and(self.with_influxdb())
            .and(warp::query::<WriteParams>())
            .and(warp::body::bytes())
            .and_then(|ctx, db, params, lines| async move {
                let request = WriteRequest::new(lines, params);
                influxdb::write(ctx, db, request).await
            });

        // Query support both get and post method, so we can't add `body_limit` here.
        // Otherwise it will throw `Rejection(LengthRequired)`
        // TODO: support body limit for POST request
        let query_api = warp::path!("query")
            .and(warp::method())
            .and(self.with_context())
            .and(self.with_influxdb())
            .and(warp::query::<InfluxqlParams>())
            .and(warp::body::form::<HashMap<String, String>>())
            .and_then(|method, ctx, db, params, body| async move {
                let request =
                    InfluxqlRequest::try_new(method, body, params).map_err(reject::custom)?;
                influxdb::query(ctx, db, HandlerQueryRequest::Influxql(request)).await
            });

        warp::path!("influxdb" / "v1" / ..).and(write_api.or(query_api))
    }

    // POST /debug/flush_memtable
    fn flush_memtable(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("debug" / "flush_memtable")
            .and(warp::post())
            .and(self.with_instance())
            .and_then(|instance: InstanceRef<Q>| async move {
                let get_all_tables = || {
                    let mut tables = Vec::new();
                    for catalog in instance
                        .catalog_manager
                        .all_catalogs()
                        .box_err()
                        .context(Internal)?
                    {
                        for schema in catalog.all_schemas().box_err().context(Internal)? {
                            for table in schema.all_tables().box_err().context(Internal)? {
                                tables.push(table);
                            }
                        }
                    }
                    Result::Ok(tables)
                };
                match get_all_tables() {
                    Ok(tables) => {
                        let mut failed_tables = Vec::new();
                        let mut success_tables = Vec::new();

                        for table in tables {
                            let table_name = table.name().to_string();
                            if let Err(e) = table.flush(FlushRequest::default()).await {
                                error!("flush {} failed, err:{}", &table_name, e);
                                failed_tables.push(table_name);
                            } else {
                                success_tables.push(table_name);
                            }
                        }
                        let mut result = HashMap::new();
                        result.insert("success", success_tables);
                        result.insert("failed", failed_tables);
                        Ok(reply::json(&result))
                    }
                    Err(e) => Err(reject::custom(e)),
                }
            })
    }

    // GET /metrics
    fn metrics(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("metrics").and(warp::get()).map(metrics::dump)
    }

    // GET /debug/heap_profile/{seconds}
    fn heap_profile(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("debug" / "heap_profile" / ..)
            .and(warp::path::param::<u64>())
            .and(warp::get())
            .and(self.with_profiler())
            .and(self.with_runtime())
            .and_then(
                |duration_sec: u64, profiler: Arc<Profiler>, runtime: Arc<Runtime>| async move {
                    let handle = runtime.spawn_blocking(move || {
                        profiler.dump_mem_prof(duration_sec).context(ProfileHeap)
                    });
                    let result = handle.await.context(JoinAsyncTask);
                    match result {
                        Ok(Ok(prof_data)) => Ok(prof_data.into_response()),
                        Ok(Err(e)) => Err(reject::custom(e)),
                        Err(e) => Err(reject::custom(e)),
                    }
                },
            )
    }

    // GET /debug/cpu_profile/{seconds}
    fn cpu_profile(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("debug" / "cpu_profile" / ..)
            .and(warp::path::param::<u64>())
            .and(warp::get())
            .and(self.with_runtime())
            .and_then(|duration_sec: u64, runtime: Arc<Runtime>| async move {
                let handle = runtime.spawn_blocking(move || -> Result<()> {
                    let guard = pprof::ProfilerGuardBuilder::default()
                        .frequency(100)
                        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
                        .build()
                        .box_err()
                        .context(Internal)?;

                    thread::sleep(Duration::from_secs(duration_sec));

                    let report = guard.report().build().box_err().context(Internal)?;
                    let file = File::create("/tmp/flamegraph.svg")
                        .box_err()
                        .context(Internal)?;
                    report.flamegraph(file).box_err().context(Internal)?;
                    Ok(())
                });
                let result = handle.await.context(JoinAsyncTask);
                match result {
                    Ok(_) => Ok("ok"),
                    Err(e) => Err(reject::custom(e)),
                }
            })
    }

    // GET /debug/config
    fn server_config(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        let server_config_content = self.config_content.clone();
        warp::path!("debug" / "config")
            .and(warp::get())
            .map(move || server_config_content.clone())
    }

    // GET /debug/stats
    fn stats(&self) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        let opened_wals = self.opened_wals.clone();
        warp::path!("debug" / "stats")
            .and(warp::get())
            .map(move || {
                [
                    "Data wal stats:",
                    &opened_wals
                        .data_wal
                        .get_statistics()
                        .unwrap_or_else(|| "Unknown".to_string()),
                    "Manifest wal stats:",
                    &opened_wals
                        .manifest_wal
                        .get_statistics()
                        .unwrap_or_else(|| "Unknown".to_string()),
                ]
                .join("\n")
            })
    }

    // PUT /debug/log_level/{level}
    fn update_log_level(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("debug" / "log_level" / String)
            .and(warp::put())
            .and(self.with_log_runtime())
            .and_then(
                |log_level: String, log_runtime: Arc<RuntimeLevel>| async move {
                    let result = log_runtime
                        .set_level_by_str(log_level.as_str())
                        .map_err(|e| Error::HandleUpdateLogLevel { msg: e });
                    match result {
                        Ok(()) => Ok(reply::json(&log_level)),
                        Err(e) => Err(reject::custom(e)),
                    }
                },
            )
    }

    // POST /admin/block
    fn admin_block(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("admin" / "block")
            .and(warp::post())
            .and(warp::body::json())
            .and(self.with_context())
            .and(self.with_instance())
            .and_then(|req, ctx, instance| async {
                let result = handlers::admin::handle_block(ctx, instance, req)
                    .await
                    .box_err()
                    .context(HandleRequest);

                match result {
                    Ok(res) => Ok(reply::json(&res)),
                    Err(e) => Err(reject::custom(e)),
                }
            })
    }

    fn with_context(
        &self,
    ) -> impl Filter<Extract = (RequestContext,), Error = warp::Rejection> + Clone {
        let default_catalog = self
            .proxy
            .instance()
            .catalog_manager
            .default_catalog_name()
            .to_string();
        let default_schema = self
            .proxy
            .instance()
            .catalog_manager
            .default_schema_name()
            .to_string();
        let timeout = self.config.timeout;

        header::optional::<String>(consts::CATALOG_HEADER)
            .and(header::optional::<String>(consts::SCHEMA_HEADER))
            .and(header::optional::<String>(consts::TENANT_HEADER))
            .and_then(
                move |catalog: Option<_>, schema: Option<_>, _tenant: Option<_>| {
                    // Clone the captured variables
                    let default_catalog = default_catalog.clone();
                    let schema = schema.unwrap_or_else(|| default_schema.clone());
                    async move {
                        RequestContext::builder()
                            .catalog(catalog.unwrap_or(default_catalog))
                            .schema(schema)
                            .timeout(timeout)
                            .enable_partition_table_access(true)
                            .build()
                            .context(CreateContext)
                            .map_err(reject::custom)
                    }
                },
            )
    }

    fn with_profiler(&self) -> impl Filter<Extract = (Arc<Profiler>,), Error = Infallible> + Clone {
        let profiler = self.profiler.clone();
        warp::any().map(move || profiler.clone())
    }

    fn with_proxy(&self) -> impl Filter<Extract = (Arc<Proxy<Q>>,), Error = Infallible> + Clone {
        let proxy = self.proxy.clone();
        warp::any().map(move || proxy.clone())
    }

    fn with_runtime(&self) -> impl Filter<Extract = (Arc<Runtime>,), Error = Infallible> + Clone {
        let runtime = self.engine_runtimes.default_runtime.clone();
        warp::any().map(move || runtime.clone())
    }

    fn with_influxdb(
        &self,
    ) -> impl Filter<Extract = (Arc<InfluxDb<Q>>,), Error = Infallible> + Clone {
        let influxdb = self.influxdb.clone();
        warp::any().map(move || influxdb.clone())
    }

    fn with_instance(
        &self,
    ) -> impl Filter<Extract = (InstanceRef<Q>,), Error = Infallible> + Clone {
        let instance = self.proxy.instance();
        warp::any().map(move || instance.clone())
    }

    fn with_router(&self) -> impl Filter<Extract = (RouterRef,), Error = Infallible> + Clone {
        let router = self.router.clone();
        warp::any().map(move || router.clone())
    }

    fn with_log_runtime(
        &self,
    ) -> impl Filter<Extract = (Arc<RuntimeLevel>,), Error = Infallible> + Clone {
        let log_runtime = self.log_runtime.clone();
        warp::any().map(move || log_runtime.clone())
    }
}

/// Service builder
pub struct Builder<Q> {
    config: HttpConfig,
    engine_runtimes: Option<Arc<EngineRuntimes>>,
    log_runtime: Option<Arc<RuntimeLevel>>,
    instance: Option<InstanceRef<Q>>,
    schema_config_provider: Option<SchemaConfigProviderRef>,
    config_content: Option<String>,
    proxy: Option<Arc<Proxy<Q>>>,
    router: Option<RouterRef>,
    opened_wals: Option<OpenedWals>,
}

impl<Q> Builder<Q> {
    pub fn new(config: HttpConfig) -> Self {
        Self {
            config,
            engine_runtimes: None,
            log_runtime: None,
            instance: None,
            schema_config_provider: None,
            config_content: None,
            proxy: None,
            router: None,
            opened_wals: None,
        }
    }

    pub fn engine_runtimes(mut self, engine_runtimes: Arc<EngineRuntimes>) -> Self {
        self.engine_runtimes = Some(engine_runtimes);
        self
    }

    pub fn log_runtime(mut self, log_runtime: Arc<RuntimeLevel>) -> Self {
        self.log_runtime = Some(log_runtime);
        self
    }

    pub fn instance(mut self, instance: InstanceRef<Q>) -> Self {
        self.instance = Some(instance);
        self
    }

    pub fn schema_config_provider(mut self, provider: SchemaConfigProviderRef) -> Self {
        self.schema_config_provider = Some(provider);
        self
    }

    pub fn config_content(mut self, content: String) -> Self {
        self.config_content = Some(content);
        self
    }

    pub fn proxy(mut self, proxy: Arc<Proxy<Q>>) -> Self {
        self.proxy = Some(proxy);
        self
    }

    pub fn router(mut self, router: RouterRef) -> Self {
        self.router = Some(router);
        self
    }

    pub fn opened_wals(mut self, opened_wals: OpenedWals) -> Self {
        self.opened_wals = Some(opened_wals);
        self
    }
}

impl<Q: QueryExecutor + 'static> Builder<Q> {
    /// Build and start the service
    pub fn build(self) -> Result<Service<Q>> {
        let engine_runtimes = self.engine_runtimes.context(MissingEngineRuntimes)?;
        let log_runtime = self.log_runtime.context(MissingLogRuntime)?;
        let instance = self.instance.context(MissingInstance)?;
        let config_content = self.config_content.context(MissingInstance)?;
        let proxy = self.proxy.context(MissingProxy)?;
        let schema_config_provider = self
            .schema_config_provider
            .context(MissingSchemaConfigProvider)?;
        let router = self.router.context(MissingRouter)?;
        let opened_wals = self.opened_wals.context(MissingWal)?;

        let influxdb = Arc::new(InfluxDb::new(instance, schema_config_provider));
        let (tx, rx) = oneshot::channel();

        let service = Service {
            proxy,
            engine_runtimes,
            log_runtime,
            influxdb,
            profiler: Arc::new(Profiler::default()),
            tx,
            rx: Some(rx),
            config: self.config.clone(),
            config_content,
            router,
            opened_wals,
        };

        Ok(service)
    }
}

/// Http service config
#[derive(Debug, Clone)]
pub struct HttpConfig {
    pub endpoint: Endpoint,
    pub max_body_size: u64,
    pub timeout: Option<Duration>,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    code: u16,
    message: String,
}

fn error_to_status_code(err: &Error) -> StatusCode {
    match err {
        Error::CreateContext { .. } => StatusCode::BAD_REQUEST,
        // TODO(yingwen): Map handle request error to more accurate status code
        Error::HandleRequest { .. }
        | Error::MissingEngineRuntimes { .. }
        | Error::MissingLogRuntime { .. }
        | Error::MissingInstance { .. }
        | Error::MissingSchemaConfigProvider { .. }
        | Error::MissingProxy { .. }
        | Error::ParseIpAddr { .. }
        | Error::ProfileHeap { .. }
        | Error::Internal { .. }
        | Error::JoinAsyncTask { .. }
        | Error::AlreadyStarted { .. }
        | Error::MissingRouter { .. }
        | Error::MissingWal { .. }
        | Error::HandleUpdateLogLevel { .. } => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

async fn handle_rejection(
    rejection: warp::Rejection,
) -> std::result::Result<(impl warp::Reply,), Infallible> {
    let code;
    let message;

    if rejection.is_not_found() {
        code = StatusCode::NOT_FOUND;
        message = String::from("NOT_FOUND");
    } else if let Some(err) = rejection.find() {
        code = error_to_status_code(err);
        let err_string = err.to_string();
        message = error_util::remove_backtrace_from_err(&err_string).to_string();
    } else {
        code = StatusCode::INTERNAL_SERVER_ERROR;
        message = error_util::remove_backtrace_from_err(&format!("UNKNOWN_ERROR: {rejection:?}"))
            .to_string();
    }

    if code.as_u16() >= 500 {
        error!("HTTP handle error: {:?}", rejection);
    }
    let json = reply::json(&ErrorResponse {
        code: code.as_u16(),
        message,
    });

    Ok((reply::with_status(json, code),))
}
