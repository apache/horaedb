// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Http service

use std::{
    collections::HashMap, convert::Infallible, error::Error as StdError, net::IpAddr, sync::Arc,
};

use catalog::manager::Manager as CatalogManager;
use log::error;
use profile::Profiler;
use query_engine::executor::Executor as QueryExecutor;
use serde_derive::Serialize;
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use table_engine::{engine::EngineRuntimes, table::FlushRequest};
use tokio::sync::oneshot::{self, Sender};
use warp::{
    header,
    http::StatusCode,
    reject,
    reply::{self, Reply},
    Filter,
};

use crate::{consts, context::RequestContext, error, handlers, instance::InstanceRef, metrics};

#[derive(Debug)]
pub struct Config {
    pub ip: String,
    pub port: u16,
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to create request context, err:{}", source))]
    CreateContext { source: crate::context::Error },

    #[snafu(display("Failed to handle request, err:{}", source))]
    HandleRequest {
        source: crate::handlers::error::Error,
    },

    #[snafu(display("Missing runtimes to build service.\nBacktrace:\n{}", backtrace))]
    MissingRuntimes { backtrace: Backtrace },

    #[snafu(display("Missing instance to build service.\nBacktrace:\n{}", backtrace))]
    MissingInstance { backtrace: Backtrace },

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
}

define_result!(Error);

impl reject::Reject for Error {}

/// Http service
///
/// Note that the service does not owns the runtime
pub struct Service<C, Q> {
    runtimes: Arc<EngineRuntimes>,
    instance: InstanceRef<C, Q>,
    profiler: Arc<Profiler>,
    tx: Sender<()>,
}

impl<C, Q> Service<C, Q> {
    // TODO(yingwen): Maybe log error or return error
    pub fn stop(self) {
        let _ = self.tx.send(());
    }
}

impl<C: CatalogManager + 'static, Q: QueryExecutor + 'static> Service<C, Q> {
    fn routes(&self) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        self.home()
            .or(self.metrics())
            .or(self.sql())
            .or(self.heap_profile())
            .or(self.admin_reject())
            .or(self.flush_memtable())
    }

    fn home(&self) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path::end().and(warp::get()).map(|| {
            let mut resp = HashMap::new();
            resp.insert("status", "ok");
            reply::json(&resp)
        })
    }

    // TODO(yingwen): Avoid boilterplate code if there are more handlers
    fn sql(&self) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("sql")
            .and(warp::post())
            // TODO(yingwen): content length limit
            .and(warp::body::json())
            .and(self.with_context())
            .and(self.with_instance())
            .and_then(|req, ctx, instance| async {
                // TODO(yingwen): Wrap common logic such as metrics, trace and error log
                let result = handlers::sql::handle_sql(ctx, instance, req)
                    .await
                    .map_err(|e| {
                        // TODO(yingwen): Maybe truncate and print the sql
                        error!("Http service Failed to handle sql, err:{}", e);
                        e
                    })
                    .context(HandleRequest);
                match result {
                    Ok(res) => Ok(reply::json(&res)),
                    Err(e) => Err(reject::custom(e)),
                }
            })
    }

    fn flush_memtable(
        &self,
    ) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("flush_memtable")
            .and(warp::post())
            .and(self.with_instance())
            .and_then(|instance: InstanceRef<C, Q>| async move {
                let get_all_tables = || {
                    let mut tables = Vec::new();
                    for catalog in instance
                        .catalog_manager
                        .all_catalogs()
                        .map_err(|e| Box::new(e) as _)
                        .context(Internal)?
                    {
                        for schema in catalog
                            .all_schemas()
                            .map_err(|e| Box::new(e) as _)
                            .context(Internal)?
                        {
                            for table in schema
                                .all_tables()
                                .map_err(|e| Box::new(e) as _)
                                .context(Internal)?
                            {
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

    fn metrics(&self) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("metrics").and(warp::get()).map(metrics::dump)
    }

    fn heap_profile(
        &self,
    ) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("debug" / "heap_profile" / ..)
            .and(warp::path::param::<u64>())
            .and(warp::get())
            .and(self.with_context())
            .and(self.with_profiler())
            .and_then(
                |duration_sec: u64, ctx: RequestContext, profiler: Arc<Profiler>| async move {
                    let handle = ctx.runtime.spawn_blocking(move || {
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

    fn with_context(
        &self,
    ) -> impl Filter<Extract = (RequestContext,), Error = warp::Rejection> + Clone {
        let default_catalog = self
            .instance
            .catalog_manager
            .default_catalog_name()
            .to_string();
        let default_schema = self
            .instance
            .catalog_manager
            .default_schema_name()
            .to_string();
        //TODO(boyan) use read/write runtime by sql type.
        let runtime = self.runtimes.bg_runtime.clone();

        header::optional::<String>(consts::CATALOG_HEADER)
            .and(header::optional::<String>(consts::TENANT_HEADER))
            .and_then(move |catalog: Option<_>, tenant: Option<_>| {
                // Clone the captured variables
                let default_catalog = default_catalog.clone();
                let default_schema = default_schema.clone();
                let runtime = runtime.clone();
                async {
                    RequestContext::builder()
                        .catalog(catalog.unwrap_or(default_catalog))
                        .tenant(tenant.unwrap_or(default_schema))
                        .runtime(runtime)
                        .build()
                        .context(CreateContext)
                        .map_err(reject::custom)
                }
            })
    }

    fn with_profiler(&self) -> impl Filter<Extract = (Arc<Profiler>,), Error = Infallible> + Clone {
        let profiler = self.profiler.clone();
        warp::any().map(move || profiler.clone())
    }

    fn with_instance(
        &self,
    ) -> impl Filter<Extract = (InstanceRef<C, Q>,), Error = Infallible> + Clone {
        let instance = self.instance.clone();
        warp::any().map(move || instance.clone())
    }

    fn admin_reject(
        &self,
    ) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("reject")
            .and(warp::post())
            .and(warp::body::json())
            .and(self.with_context())
            .and(self.with_instance())
            .and_then(|req, ctx, instance| async {
                let result = handlers::admin::handle_reject(ctx, instance, req)
                    .await
                    .map_err(|e| {
                        error!("Http service failed to handle admin reject, err:{}", e);
                        e
                    })
                    .context(HandleRequest);

                match result {
                    Ok(res) => Ok(reply::json(&res)),
                    Err(e) => Err(reject::custom(e)),
                }
            })
    }
}

/// Service builder
pub struct Builder<C, Q> {
    config: Config,
    runtimes: Option<Arc<EngineRuntimes>>,
    instance: Option<InstanceRef<C, Q>>,
}

impl<C, Q> Builder<C, Q> {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            runtimes: None,
            instance: None,
        }
    }

    pub fn runtimes(mut self, runtimes: Arc<EngineRuntimes>) -> Self {
        self.runtimes = Some(runtimes);
        self
    }

    pub fn instance(mut self, instance: InstanceRef<C, Q>) -> Self {
        self.instance = Some(instance);
        self
    }
}

impl<C: CatalogManager + 'static, Q: QueryExecutor + 'static> Builder<C, Q> {
    /// Build and start the service
    pub fn build(self) -> Result<Service<C, Q>> {
        let runtimes = self.runtimes.context(MissingRuntimes)?;
        let instance = self.instance.context(MissingInstance)?;
        let (tx, rx) = oneshot::channel();

        let service = Service {
            runtimes: runtimes.clone(),
            instance,
            profiler: Arc::new(Profiler::default()),
            tx,
        };

        let ip_addr: IpAddr = self
            .config
            .ip
            .parse()
            .context(ParseIpAddr { ip: self.config.ip })?;

        // Register filters to warp and rejection handler
        let routes = service.routes().recover(handle_rejection);
        let (_addr, server) =
            warp::serve(routes).bind_with_graceful_shutdown((ip_addr, self.config.port), async {
                rx.await.ok();
            });
        // Run the service
        runtimes.bg_runtime.spawn(server);

        Ok(service)
    }
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
        | Error::MissingRuntimes { .. }
        | Error::MissingInstance { .. }
        | Error::ParseIpAddr { .. }
        | Error::ProfileHeap { .. }
        | Error::Internal { .. }
        | Error::JoinAsyncTask { .. } => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

async fn handle_rejection(
    rejection: warp::Rejection,
) -> std::result::Result<impl warp::Reply, Infallible> {
    let code;
    let message;

    if rejection.is_not_found() {
        code = StatusCode::NOT_FOUND;
        message = String::from("NOT_FOUND");
    } else if let Some(err) = rejection.find() {
        code = error_to_status_code(err);
        let err_string = err.to_string();
        message = error::first_line_in_error(&err_string).to_string();
    } else {
        error!("handle error: {:?}", rejection);
        code = StatusCode::INTERNAL_SERVER_ERROR;
        message = format!("UNKNOWN_ERROR: {:?}", rejection);
    }

    let json = reply::json(&ErrorResponse {
        code: code.as_u16(),
        message,
    });

    Ok(reply::with_status(json, code))
}
