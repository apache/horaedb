// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

// Meta event rpc service implementation.

use std::{sync::Arc, time::Instant};

use analytic_engine::setup::OpenedWals;
use async_trait::async_trait;
use catalog::{
    schema::{
        CloseOptions, CloseTableRequest, CreateOptions, CreateTableRequest, DropOptions,
        DropTableRequest, OpenOptions, OpenTableRequest, TableDef,
    },
    table_operator::TableOperator,
};
use ceresdbproto::meta_event::{
    meta_event_service_server::MetaEventService, ChangeShardRoleRequest,
    ChangeShardRoleResponse, CloseShardRequest, CloseShardResponse, CloseTableOnShardRequest,
    CloseTableOnShardResponse, CreateTableOnShardRequest, CreateTableOnShardResponse,
    DropTableOnShardRequest, DropTableOnShardResponse, MergeShardsRequest, MergeShardsResponse,
    OpenShardRequest, OpenShardResponse, OpenTableOnShardRequest, OpenTableOnShardResponse,
    SplitShardRequest, SplitShardResponse,
};
use cluster::{shard_set::UpdatedTableInfo, ClusterRef};
use common_types::{schema::SchemaEncoder, table::ShardId};
use common_util::{
    error::{BoxError},
    runtime::Runtime,
    time::InstantExt,
};
use log::{error, info, warn};
use meta_client::types::{ShardInfo, TableInfo};
use paste::paste;
use proxy::instance::InstanceRef;
use query_engine::executor::Executor as QueryExecutor;
use snafu::{OptionExt, ResultExt};
use table_engine::{
    engine::{TableEngineRef, TableState},
    partition::PartitionInfo,
    table::TableId,
    ANALYTIC_ENGINE_TYPE,
};
use tonic::Response;

use self::shard_operation::WalCloserAdapter;
use crate::grpc::{
    meta_event_service::{
        error::{ErrNoCause, ErrWithCause, Result, StatusCode},
        shard_operation::WalRegionCloserRef,
    },
    metrics::META_EVENT_GRPC_HANDLER_DURATION_HISTOGRAM_VEC,
};

mod error;
mod shard_operation;

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

    // Success to lock the shard in this node, fetch and open shard now.
    let shard = ctx
        .cluster
        .open_shard(&shard_info)
        .await
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: "fail to open shards in cluster",
        })?;

    // Lock the shard in local, and then recover it.
    let shard = shard.write().await;

    let catalog_name = &ctx.default_catalog;
    let shard_info = shard.shard_info.clone();
    let table_defs = shard
        .tables
        .iter()
        .map(|info| TableDef {
            catalog_name: catalog_name.clone(),
            schema_name: info.schema_name.clone(),
            id: TableId::from(info.id),
            name: info.name.clone(),
        })
        .collect();

    let open_shard_request = catalog::schema::OpenShardRequest {
        shard_id: shard_info.id,
        table_defs,
        engine: ANALYTIC_ENGINE_TYPE.to_string(),
    };
    let opts = OpenOptions {
        table_engine: ctx.table_engine,
    };

    ctx.table_operator
        .open_shard(open_shard_request, opts)
        .await
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: "failed to open shard",
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
    let shard = ctx
        .cluster
        .get_shard(shard_id)
        .with_context(|| ErrNoCause {
            code: StatusCode::Internal,
            msg: format!("shard not found when closing shard, shard_id:{shard_id}",),
        })?;

    let mut shard = shard.write().await;

    shard.freeze();

    info!("Shard is frozen before closed, shard_id:{shard_id}");

    let catalog_name = &ctx.default_catalog.clone();
    let shard_info = shard.shard_info.clone();
    let table_defs = shard
        .tables
        .iter()
        .map(|info| TableDef {
            catalog_name: catalog_name.clone(),
            schema_name: info.schema_name.clone(),
            id: TableId::from(info.id),
            name: info.name.clone(),
        })
        .collect();
    let close_shard_request = catalog::schema::CloseShardRequest {
        shard_id: shard_info.id,
        table_defs,
        engine: ANALYTIC_ENGINE_TYPE.to_string(),
    };
    let opts = CloseOptions {
        table_engine: ctx.table_engine.clone(),
    };

    ctx.table_operator
        .close_shard(close_shard_request, opts)
        .await
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: "failed to close shard",
        })?;

    // Try to close wal region
    ctx.wal_region_closer
        .close_region(shard_id)
        .await
        .with_context(|| ErrWithCause {
            code: StatusCode::Internal,
            msg: format!("fail to close wal region, shard_id:{shard_id}"),
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
    })
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
        .get_shard(updated_table_info.shard_info.id)
        .with_context(|| ErrNoCause {
            code: StatusCode::Internal,
            msg: format!(
                "shard not found when creating table on shard, req:{:?}",
                request.clone()
            ),
        })?;

    let mut shard = shard.write().await;

    // FIXME: should insert table from cluster after having created table.
    shard
        .try_insert_table(updated_table_info)
        .box_err()
        .with_context(|| ErrWithCause {
            code: StatusCode::Internal,
            msg: format!(
                "fail to insert table to cluster when creating table on shard, req:{:?}",
                request.clone()
            ),
        })?;

    // Create the table by operator afterwards.
    let catalog_name = &ctx.default_catalog;
    let shard_info = request
        .update_shard_info
        .context(ErrNoCause {
            code: StatusCode::BadRequest,
            msg: "update shard info is missing in the CreateTableOnShardRequest",
        })?
        .curr_shard_info
        .context(ErrNoCause {
            code: StatusCode::BadRequest,
            msg: "current shard info is missing ine CreateTableOnShardRequest",
        })?;
    let table_info = request.table_info.context(ErrNoCause {
        code: StatusCode::BadRequest,
        msg: "table info is missing in the CreateTableOnShardRequest",
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

    let (table_engine, partition_info) = match table_info.partition_info {
        Some(v) => {
            let partition_info = Some(PartitionInfo::try_from(v.clone()).box_err().with_context(
                || ErrWithCause {
                    code: StatusCode::BadRequest,
                    msg: format!("fail to parse partition info, partition_info:{v:?}"),
                },
            )?);
            (ctx.partition_table_engine.clone(), partition_info)
        }
        None => (ctx.table_engine.clone(), None),
    };

    // Build create table request and options.
    let create_table_request = CreateTableRequest {
        catalog_name: catalog_name.clone(),
        schema_name: table_info.schema_name,
        table_name: table_info.name,
        table_id: Some(TableId::new(table_info.id)),
        table_schema,
        engine: request.engine,
        options: request.options,
        state: TableState::Stable,
        shard_id: shard_info.id,
        partition_info,
    };

    let create_opts = CreateOptions {
        table_engine,
        create_if_not_exists: request.create_if_not_exist,
    };

    let _ = ctx
        .table_operator
        .create_table_on_shard(create_table_request.clone(), create_opts)
        .await
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: format!("fail to create table with request:{create_table_request:?}"),
        })?;

    Ok(())
}

async fn handle_drop_table_on_shard(
    ctx: HandlerContext,
    request: DropTableOnShardRequest,
) -> Result<()> {
    let updated_table_info = extract_updated_table_info!(request);
    let shard = ctx
        .cluster
        .get_shard(updated_table_info.shard_info.id)
        .with_context(|| ErrNoCause {
            code: StatusCode::Internal,
            msg: format!(
                "shard not found when dropping table on shard, req:{:?}",
                request.clone()
            ),
        })?;

    let mut shard = shard.write().await;

    // FIXME: should insert table from cluster after having dropped table.
    shard
        .try_remove_table(updated_table_info)
        .box_err()
        .with_context(|| ErrWithCause {
            code: StatusCode::Internal,
            msg: format!(
                "fail to remove table to cluster when dropping table on shard, req:{:?}",
                request.clone()
            ),
        })?;

    // Drop the table by operator afterwards.
    let catalog_name = ctx.default_catalog.clone();
    let table_info = request.table_info.context(ErrNoCause {
        code: StatusCode::BadRequest,
        msg: "table info is missing in the DropTableOnShardRequest",
    })?;
    let drop_table_request = DropTableRequest {
        catalog_name,
        schema_name: table_info.schema_name,
        table_name: table_info.name,
        // FIXME: the engine type should not use the default one.
        engine: ANALYTIC_ENGINE_TYPE.to_string(),
    };
    let drop_opts = DropOptions {
        table_engine: ctx.table_engine,
    };

    ctx.table_operator
        .drop_table_on_shard(drop_table_request.clone(), drop_opts)
        .await
        .box_err()
        .with_context(|| ErrWithCause {
            code: StatusCode::Internal,
            msg: format!("fail to drop table with request:{drop_table_request:?}"),
        })?;

    Ok(())
}

async fn handle_open_table_on_shard(
    ctx: HandlerContext,
    request: OpenTableOnShardRequest,
) -> Result<()> {
    let updated_table_info = extract_updated_table_info!(request);
    let shard = ctx
        .cluster
        .get_shard(updated_table_info.shard_info.id)
        .with_context(|| ErrNoCause {
            code: StatusCode::Internal,
            msg: format!(
                "shard not found when opening table on shard, req:{:?}",
                request.clone()
            ),
        })?;

    let mut shard = shard.write().await;

    // FIXME: should insert table from cluster after having opened table.
    shard
        .try_insert_table(updated_table_info)
        .box_err()
        .with_context(|| ErrWithCause {
            code: StatusCode::Internal,
            msg: format!(
                "fail to insert table to cluster when opening table on shard, req:{:?}",
                request.clone()
            ),
        })?;

    // Open the table by operator afterwards.
    let catalog_name = ctx.default_catalog.clone();
    let shard_info = request
        .update_shard_info
        .context(ErrNoCause {
            code: StatusCode::BadRequest,
            msg: "update shard info is missing in the OpenTableOnShardRequest",
        })?
        .curr_shard_info
        .context(ErrNoCause {
            code: StatusCode::BadRequest,
            msg: "current shard info is missing ine OpenTableOnShardRequest",
        })?;
    let table_info = request.table_info.context(ErrNoCause {
        code: StatusCode::BadRequest,
        msg: "table info is missing in the OpenTableOnShardRequest",
    })?;
    let open_table_request = OpenTableRequest {
        catalog_name,
        schema_name: table_info.schema_name,
        table_name: table_info.name,
        // FIXME: the engine type should not use the default one.
        engine: ANALYTIC_ENGINE_TYPE.to_string(),
        shard_id: shard_info.id,
        table_id: TableId::new(table_info.id),
    };
    let open_opts = OpenOptions {
        table_engine: ctx.table_engine,
    };

    ctx.table_operator
        .open_table_on_shard(open_table_request.clone(), open_opts)
        .await
        .box_err()
        .with_context(|| ErrWithCause {
            code: StatusCode::Internal,
            msg: format!("fail to open table with request:{open_table_request:?}"),
        })?;

    Ok(())
}

async fn handle_close_table_on_shard(
    ctx: HandlerContext,
    request: CloseTableOnShardRequest,
) -> Result<()> {
    let updated_table_info = extract_updated_table_info!(request);
    let shard = ctx
        .cluster
        .get_shard(updated_table_info.shard_info.id)
        .with_context(|| ErrNoCause {
            code: StatusCode::Internal,
            msg: format!(
                "shard not found when closing table on shard, req:{:?}",
                request.clone()
            ),
        })?;

    let mut shard = shard.write().await;

    // FIXME: should remove table from cluster after having closed table.
    shard
        .try_remove_table(updated_table_info)
        .box_err()
        .with_context(|| ErrWithCause {
            code: StatusCode::Internal,
            msg: format!(
                "fail to remove table from cluster when closing table on shard, req:{:?}",
                request.clone()
            ),
        })?;

    // Close the table by catalog manager afterwards.
    let catalog_name = &ctx.default_catalog;
    let table_info = request.table_info.context(ErrNoCause {
        code: StatusCode::BadRequest,
        msg: "table info is missing in the CloseTableOnShardRequest",
    })?;
    let close_table_request = CloseTableRequest {
        catalog_name: catalog_name.clone(),
        schema_name: table_info.schema_name,
        table_name: table_info.name,
        table_id: TableId::new(table_info.id),
        // FIXME: the engine type should not use the default one.
        engine: ANALYTIC_ENGINE_TYPE.to_string(),
    };
    let close_opts = CloseOptions {
        table_engine: ctx.table_engine,
    };

    ctx.table_operator
        .close_table_on_shard(close_table_request.clone(), close_opts)
        .await
        .box_err()
        .with_context(|| ErrWithCause {
            code: StatusCode::Internal,
            msg: format!("fail to close table with request:{close_table_request:?}"),
        })?;

    Ok(())
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
