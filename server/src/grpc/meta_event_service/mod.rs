// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

// Meta event rpc service implementation.

use std::{sync::Arc, time::Instant};

use analytic_engine::setup::OpenedWals;
use async_trait::async_trait;
use catalog::table_operator::TableOperator;
use ceresdbproto::meta_event::{
    meta_event_service_server::MetaEventService, ChangeShardRoleRequest, ChangeShardRoleResponse,
    CloseShardRequest, CloseShardResponse, CloseTableOnShardRequest, CloseTableOnShardResponse,
    CreateTableOnShardRequest, CreateTableOnShardResponse, DropTableOnShardRequest,
    DropTableOnShardResponse, MergeShardsRequest, MergeShardsResponse, OpenShardRequest,
    OpenShardResponse, OpenTableOnShardRequest, OpenTableOnShardResponse, SplitShardRequest,
    SplitShardResponse,
};
use cluster::{
    shard_operation::{WalCloserAdapter, WalRegionCloserRef},
    shard_operator::{
        CloseContext, CloseTableContext, CreateTableContext, DropTableContext, OpenContext,
        OpenTableContext,
    },
    shard_set::UpdatedTableInfo,
    ClusterRef,
};
use common_types::{schema::SchemaEncoder, table::ShardId};
use common_util::{error::BoxError, runtime::Runtime, time::InstantExt};
use log::{error, info, warn};
use meta_client::types::{ShardInfo, TableInfo};
use paste::paste;
use proxy::instance::InstanceRef;
use query_engine::executor::Executor as QueryExecutor;
use snafu::{OptionExt, ResultExt};
use table_engine::{engine::TableEngineRef, ANALYTIC_ENGINE_TYPE};
use tonic::Response;

use crate::grpc::{
    meta_event_service::error::{ErrNoCause, ErrWithCause, Result, StatusCode},
    metrics::META_EVENT_GRPC_HANDLER_DURATION_HISTOGRAM_VEC,
};

mod error;

macro_rules! extract_updated_table_info {
    ($request: expr) => {{
        let update_shard_info = $request.update_shard_info.clone();
        let table_info = $request.table_info.clone();

        let update_shard_info = update_shard_info.context(ErrNoCause {
            code: StatusCode::Internal,
            msg: "update shard info is missing",
        })?;
        let curr_shard_info = update_shard_info.curr_shard_info.context(ErrNoCause {
            code: StatusCode::Internal,
            msg: "update shard info is missing",
        })?;
        let table_info = table_info.context(ErrNoCause {
            code: StatusCode::Internal,
            msg: "update shard info is missing",
        })?;

        let prev_version = update_shard_info.prev_version;
        let shard_info = ShardInfo::from(&curr_shard_info);
        let table_info = TableInfo::try_from(table_info)
            .box_err()
            .context(ErrWithCause {
                code: StatusCode::Internal,
                msg: "failed to parse tableInfo",
            })?;

        UpdatedTableInfo {
            prev_version,
            shard_info,
            table_info,
        }
    }};
}

/// Builder for [MetaServiceImpl].
pub struct Builder<Q> {
    pub cluster: ClusterRef,
    pub instance: InstanceRef<Q>,
    pub runtime: Arc<Runtime>,
    pub opened_wals: OpenedWals,
}

impl<Q: QueryExecutor + 'static> Builder<Q> {
    pub fn build(self) -> MetaServiceImpl<Q> {
        let Self {
            cluster,
            instance,
            runtime,
            opened_wals,
        } = self;

        MetaServiceImpl {
            cluster,
            instance,
            runtime,
            wal_region_closer: Arc::new(WalCloserAdapter {
                data_wal: opened_wals.data_wal,
                manifest_wal: opened_wals.manifest_wal,
            }),
        }
    }
}

#[derive(Clone)]
pub struct MetaServiceImpl<Q: QueryExecutor + 'static> {
    cluster: ClusterRef,
    instance: InstanceRef<Q>,
    runtime: Arc<Runtime>,
    wal_region_closer: WalRegionCloserRef,
}

macro_rules! handle_request {
    ($mod_name: ident, $req_ty: ident, $resp_ty: ident) => {
        paste! {
            async fn [<$mod_name _internal>] (
                &self,
                request: tonic::Request<$req_ty>,
            ) -> std::result::Result<tonic::Response<$resp_ty>, tonic::Status> {
                let instant = Instant::now();
                let ctx = self.handler_ctx();
                let handle = self.runtime.spawn(async move {
                    // FIXME: Data race about the operations on the shards should be taken into
                    // considerations.

                    let request = request.into_inner();
                    info!("Receive request from meta, req:{:?}", request);

                    [<handle_ $mod_name>](ctx, request).await
                });

                let res = handle
                    .await
                    .box_err()
                    .context(ErrWithCause {
                        code: StatusCode::Internal,
                        msg: "fail to join task",
                    });

                let mut resp = $resp_ty::default();
                match res {
                    Ok(Ok(_)) => {
                        resp.header = Some(error::build_ok_header());
                    }
                    Ok(Err(e)) | Err(e) => {
                        error!("Fail to process request from meta, err:{}", e);
                        resp.header = Some(error::build_err_header(e));
                    }
                };

                info!("Finish handling request from meta, resp:{:?}", resp);

                META_EVENT_GRPC_HANDLER_DURATION_HISTOGRAM_VEC
                    .$mod_name
                    .observe(instant.saturating_elapsed().as_secs_f64());
                Ok(Response::new(resp))
            }
        }
    };
}

impl<Q: QueryExecutor + 'static> MetaServiceImpl<Q> {
    handle_request!(open_shard, OpenShardRequest, OpenShardResponse);

    handle_request!(close_shard, CloseShardRequest, CloseShardResponse);

    handle_request!(
        create_table_on_shard,
        CreateTableOnShardRequest,
        CreateTableOnShardResponse
    );

    handle_request!(
        drop_table_on_shard,
        DropTableOnShardRequest,
        DropTableOnShardResponse
    );

    handle_request!(
        open_table_on_shard,
        OpenTableOnShardRequest,
        OpenTableOnShardResponse
    );

    handle_request!(
        close_table_on_shard,
        CloseTableOnShardRequest,
        CloseTableOnShardResponse
    );

    fn handler_ctx(&self) -> HandlerContext {
        HandlerContext {
            cluster: self.cluster.clone(),
            default_catalog: self
                .instance
                .catalog_manager
                .default_catalog_name()
                .to_string(),
            table_operator: TableOperator::new(self.instance.catalog_manager.clone()),
            table_engine: self.instance.table_engine.clone(),
            partition_table_engine: self.instance.partition_table_engine.clone(),
            wal_region_closer: self.wal_region_closer.clone(),
        }
    }
}

/// Context for handling all kinds of meta event service.
#[derive(Clone)]
struct HandlerContext {
    cluster: ClusterRef,
    default_catalog: String,
    table_operator: TableOperator,
    table_engine: TableEngineRef,
    partition_table_engine: TableEngineRef,
    wal_region_closer: WalRegionCloserRef,
}

impl HandlerContext {
    async fn acquire_shard_lock(&self, shard_id: ShardId) -> Result<()> {
        let lock_mgr = self.cluster.shard_lock_manager();
        let new_ctx = self.clone();
        let on_lock_expired = |shard_id| async move {
            warn!("Shard lock is released, try to close the tables and shard, shard_id:{shard_id}");
            let res = do_close_shard(&new_ctx, shard_id).await;
            match res {
                Ok(_) => info!("Close shard success, shard_id:{shard_id}"),
                Err(e) => {
                    panic!(
                        "Close shard failed, and we have to panic to ensure the data \
                        integrity, shard_id:{shard_id}, err:{e}"
                    );
                }
            }
        };

        let granted_by_this_call = lock_mgr
            .grant_lock(shard_id, on_lock_expired)
            .await
            .box_err()
            .context(ErrWithCause {
                code: StatusCode::Internal,
                msg: "fail to acquire shard lock",
            })?;

        if !granted_by_this_call {
            warn!("Shard lock is already granted, shard_id:{}", shard_id);
        }

        Ok(())
    }

    async fn release_shard_lock(&self, shard_id: ShardId) -> Result<()> {
        let lock_mgr = self.cluster.shard_lock_manager();
        let revoked_by_this_call =
            lock_mgr
                .revoke_lock(shard_id)
                .await
                .box_err()
                .context(ErrWithCause {
                    code: StatusCode::Internal,
                    msg: "fail to release shard lock",
                })?;

        if revoked_by_this_call {
            warn!("Shard lock is revoked already, shard_id:{}", shard_id);
        }

        Ok(())
    }
}

async fn do_open_shard(ctx: HandlerContext, shard_info: ShardInfo) -> Result<()> {
    // Try to lock the shard in node level.
    ctx.acquire_shard_lock(shard_info.id).await?;

    let shard = ctx
        .cluster
        .open_shard(&shard_info)
        .await
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: "fail to open shards in cluster",
        })?;

    let open_ctx = OpenContext {
        catalog: ctx.default_catalog.clone(),
        table_engine: ctx.table_engine.clone(),
        table_operator: ctx.table_operator.clone(),
        // FIXME: the engine type should not use the default one.
        engine: ANALYTIC_ENGINE_TYPE.to_string(),
    };

    shard.open(open_ctx).await.box_err().context(ErrWithCause {
        code: StatusCode::Internal,
        msg: "fail to open shard",
    })
}

// TODO: maybe we should encapsulate the logic of handling meta event into a
// trait, so that we don't need to expose the logic to the meta event service
// implementation.
async fn handle_open_shard(ctx: HandlerContext, request: OpenShardRequest) -> Result<()> {
    info!("Receive open shard request, request:{request:?}");
    let shard_info = ShardInfo::from(&request.shard.context(ErrNoCause {
        code: StatusCode::BadRequest,
        msg: "shard info is required",
    })?);

    let shard_id = shard_info.id;
    info!("Handle open shard begins, shard_id:{shard_id}");
    match do_open_shard(ctx, shard_info).await {
        Err(e) => {
            error!("Failed to open shard, shard_id:{shard_id}, err:{e}");
            Err(e)
        }
        Ok(_) => {
            info!("Handle open shard success, shard_id:{shard_id}");
            Ok(())
        }
    }
}

async fn do_close_shard(ctx: &HandlerContext, shard_id: ShardId) -> Result<()> {
    let shard = ctx.cluster.shard(shard_id).with_context(|| ErrNoCause {
        code: StatusCode::Internal,
        msg: format!("shard not found when closing shard, shard_id:{shard_id}",),
    })?;

    let close_ctx = CloseContext {
        catalog: ctx.default_catalog.clone(),
        table_engine: ctx.table_engine.clone(),
        table_operator: ctx.table_operator.clone(),
        wal_region_closer: ctx.wal_region_closer.clone(),
        // FIXME: the engine type should not use the default one.
        engine: ANALYTIC_ENGINE_TYPE.to_string(),
    };

    shard
        .close(close_ctx)
        .await
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: "fail to close shard",
        })?;

    // Remove the shard from the cluster topology after the shard is closed indeed.
    let _ = ctx
        .cluster
        .close_shard(shard_id)
        .await
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: "fail to close shards in cluster",
        })?;

    ctx.release_shard_lock(shard_id).await.map_err(|e| {
        error!("Failed to release shard lock, shard_id:{shard_id}, err:{e}");
        e
    })?;

    info!("Shard close sequentially finish, shard_id:{shard_id}");

    Ok(())
}

async fn handle_close_shard(ctx: HandlerContext, request: CloseShardRequest) -> Result<()> {
    info!("Receive close shard request, request:{request:?}");

    let shard_id = request.shard_id;
    info!("Handle close shard begins, shard_id:{shard_id}");
    match do_close_shard(&ctx, shard_id).await {
        Ok(_) => {
            info!("Handle close shard succeed, shard_id:{shard_id}");
            Ok(())
        }
        Err(e) => {
            error!("Failed to close shard, shard_id:{shard_id}, err:{e}");
            Err(e)
        }
    }
}

async fn handle_create_table_on_shard(
    ctx: HandlerContext,
    request: CreateTableOnShardRequest,
) -> Result<()> {
    let updated_table_info = extract_updated_table_info!(request);

    let shard = ctx
        .cluster
        .shard(updated_table_info.shard_info.id)
        .with_context(|| ErrNoCause {
            code: StatusCode::Internal,
            msg: format!(
                "shard not found when creating table on shard, req:{:?}",
                request.clone()
            ),
        })?;

    // Get information for partition table creating.
    let table_schema = SchemaEncoder::default()
        .decode(&request.encoded_schema)
        .box_err()
        .with_context(|| ErrWithCause {
            code: StatusCode::BadRequest,
            msg: format!(
                "fail to decode encoded schema bytes, raw_bytes:{:?}",
                request.encoded_schema
            ),
        })?;

    let create_table_ctx = CreateTableContext {
        catalog: ctx.default_catalog.clone(),
        table_engine: ctx.table_engine.clone(),
        table_operator: ctx.table_operator.clone(),
        partition_table_engine: ctx.partition_table_engine.clone(),
        updated_table_info,
        table_schema,
        options: request.options,
        create_if_not_exist: request.create_if_not_exist,
        engine: request.engine,
    };

    shard
        .create_table(create_table_ctx)
        .await
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: "fail to create table on shard",
        })
}

async fn handle_drop_table_on_shard(
    ctx: HandlerContext,
    request: DropTableOnShardRequest,
) -> Result<()> {
    let updated_table_info = extract_updated_table_info!(request);

    let shard = ctx
        .cluster
        .shard(updated_table_info.shard_info.id)
        .with_context(|| ErrNoCause {
            code: StatusCode::Internal,
            msg: format!(
                "shard not found when dropping table on shard, req:{:?}",
                request.clone()
            ),
        })?;

    let drop_table_ctx = DropTableContext {
        catalog: ctx.default_catalog.clone(),
        table_engine: ctx.table_engine.clone(),
        table_operator: ctx.table_operator.clone(),
        updated_table_info,
        // FIXME: the engine type should not use the default one.
        engine: ANALYTIC_ENGINE_TYPE.to_string(),
    };

    shard
        .drop_table(drop_table_ctx)
        .await
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: "fail to drop table on shard",
        })
}

async fn handle_open_table_on_shard(
    ctx: HandlerContext,
    request: OpenTableOnShardRequest,
) -> Result<()> {
    let updated_table_info = extract_updated_table_info!(request);

    let shard = ctx
        .cluster
        .shard(updated_table_info.shard_info.id)
        .with_context(|| ErrNoCause {
            code: StatusCode::Internal,
            msg: format!("shard not found when opening table on shard, request:{request:?}",),
        })?;

    let open_table_ctx = OpenTableContext {
        catalog: ctx.default_catalog.clone(),
        table_engine: ctx.table_engine.clone(),
        table_operator: ctx.table_operator.clone(),
        updated_table_info,
        // FIXME: the engine type should not use the default one.
        engine: ANALYTIC_ENGINE_TYPE.to_string(),
    };

    shard
        .open_table(open_table_ctx)
        .await
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: "fail to open table on shard",
        })
}

async fn handle_close_table_on_shard(
    ctx: HandlerContext,
    request: CloseTableOnShardRequest,
) -> Result<()> {
    let updated_table_info = extract_updated_table_info!(request);
    let shard_id = updated_table_info.shard_info.id;

    let shard = ctx.cluster.shard(shard_id).with_context(|| ErrNoCause {
        code: StatusCode::Internal,
        msg: format!(
            "shard not found when closing table on shard, req:{:?}",
            request.clone()
        ),
    })?;

    let close_table_ctx = CloseTableContext {
        catalog: ctx.default_catalog.clone(),
        table_engine: ctx.table_engine.clone(),
        table_operator: ctx.table_operator.clone(),
        updated_table_info,
        // FIXME: the engine type should not use the default one.
        engine: ANALYTIC_ENGINE_TYPE.to_string(),
    };

    shard
        .close_table(close_table_ctx)
        .await
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: "fail to close table on shard",
        })
}

#[async_trait]
impl<Q: QueryExecutor + 'static> MetaEventService for MetaServiceImpl<Q> {
    async fn open_shard(
        &self,
        request: tonic::Request<OpenShardRequest>,
    ) -> std::result::Result<tonic::Response<OpenShardResponse>, tonic::Status> {
        self.open_shard_internal(request).await
    }

    async fn close_shard(
        &self,
        request: tonic::Request<CloseShardRequest>,
    ) -> std::result::Result<tonic::Response<CloseShardResponse>, tonic::Status> {
        self.close_shard_internal(request).await
    }

    async fn create_table_on_shard(
        &self,
        request: tonic::Request<CreateTableOnShardRequest>,
    ) -> std::result::Result<tonic::Response<CreateTableOnShardResponse>, tonic::Status> {
        self.create_table_on_shard_internal(request).await
    }

    async fn drop_table_on_shard(
        &self,
        request: tonic::Request<DropTableOnShardRequest>,
    ) -> std::result::Result<tonic::Response<DropTableOnShardResponse>, tonic::Status> {
        self.drop_table_on_shard_internal(request).await
    }

    async fn open_table_on_shard(
        &self,
        request: tonic::Request<OpenTableOnShardRequest>,
    ) -> std::result::Result<tonic::Response<OpenTableOnShardResponse>, tonic::Status> {
        self.open_table_on_shard_internal(request).await
    }

    async fn close_table_on_shard(
        &self,
        request: tonic::Request<CloseTableOnShardRequest>,
    ) -> std::result::Result<tonic::Response<CloseTableOnShardResponse>, tonic::Status> {
        self.close_table_on_shard_internal(request).await
    }

    async fn split_shard(
        &self,
        request: tonic::Request<SplitShardRequest>,
    ) -> std::result::Result<tonic::Response<SplitShardResponse>, tonic::Status> {
        info!("Receive split shard request:{:?}", request);
        return Err(tonic::Status::new(tonic::Code::Unimplemented, ""));
    }

    async fn merge_shards(
        &self,
        request: tonic::Request<MergeShardsRequest>,
    ) -> std::result::Result<tonic::Response<MergeShardsResponse>, tonic::Status> {
        info!("Receive merge shards request:{:?}", request);
        return Err(tonic::Status::new(tonic::Code::Unimplemented, ""));
    }

    async fn change_shard_role(
        &self,
        request: tonic::Request<ChangeShardRoleRequest>,
    ) -> std::result::Result<tonic::Response<ChangeShardRoleResponse>, tonic::Status> {
        info!("Receive change shard role request:{:?}", request);
        return Err(tonic::Status::new(tonic::Code::Unimplemented, ""));
    }
}
