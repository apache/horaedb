// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

// Meta event rpc service implementation.

use std::sync::Arc;

use async_trait::async_trait;
use catalog::{
    manager::ManagerRef,
    schema::{
        CloseOptions, CreateOptions, CreateTableRequest, DropOptions, DropTableRequest,
        OpenOptions, OpenTableRequest,
    },
    CatalogRef,
};
use ceresdbproto::meta_event::{
    meta_event_service_server::MetaEventService, ChangeShardRoleRequest, ChangeShardRoleResponse,
    CloseShardRequest, CloseShardResponse, CreateTableOnShardRequest, CreateTableOnShardResponse,
    DropTableOnShardRequest, DropTableOnShardResponse, MergeShardsRequest, MergeShardsResponse,
    OpenShardRequest, OpenShardResponse, SplitShardRequest, SplitShardResponse,
};
use cluster::ClusterRef;
use common_types::schema::SchemaEncoder;
use common_util::runtime::Runtime;
use log::info;
use paste::paste;
use query_engine::executor::Executor as QueryExecutor;
use snafu::{OptionExt, ResultExt};
use table_engine::{
    engine::{CloseTableRequest, TableEngineRef, TableState},
    table::{SchemaId, TableId},
    ANALYTIC_ENGINE_TYPE,
};

use crate::{
    grpc::meta_event_service::error::{ErrNoCause, ErrWithCause, Result, StatusCode},
    instance::InstanceRef,
};

pub(crate) mod error;

#[derive(Clone)]
pub struct MetaServiceImpl<Q: QueryExecutor + 'static> {
    pub cluster: ClusterRef,
    pub instance: InstanceRef<Q>,
    pub runtime: Arc<Runtime>,
}

macro_rules! handle_request {
    ($mod_name: ident, $req_ty: ident, $resp_ty: ident) => {
        paste! {
            async fn [<$mod_name _internal>] (
                &self,
                request: tonic::Request<$req_ty>,
            ) -> std::result::Result<tonic::Response<$resp_ty>, tonic::Status> {
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
                    .map_err(|e| Box::new(e) as _)
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
                        resp.header = Some(error::build_err_header(e));
                    }
                };

                info!("Finish handling request from meta, resp:{:?}", resp);
                Ok(tonic::Response::new(resp))
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

    fn handler_ctx(&self) -> HandlerContext {
        HandlerContext {
            cluster: self.cluster.clone(),
            catalog_manager: self.instance.catalog_manager.clone(),
            table_engine: self.instance.table_engine.clone(),
        }
    }
}

/// Context for handling all kinds of meta event service.
struct HandlerContext {
    cluster: ClusterRef,
    catalog_manager: ManagerRef,
    table_engine: TableEngineRef,
}

impl HandlerContext {
    fn default_catalog(&self) -> Result<CatalogRef> {
        let default_catalog_name = self.catalog_manager.default_catalog_name();
        let default_catalog = self
            .catalog_manager
            .catalog_by_name(default_catalog_name)
            .map_err(|e| Box::new(e) as _)
            .context(ErrWithCause {
                code: StatusCode::Internal,
                msg: "fail to get default catalog",
            })?
            .context(ErrNoCause {
                code: StatusCode::NotFound,
                msg: "default catalog is not found",
            })?;

        Ok(default_catalog)
    }
}

async fn handle_open_shard(ctx: HandlerContext, request: OpenShardRequest) -> Result<()> {
    let tables_of_shard = ctx
        .cluster
        .open_shard(&request)
        .await
        .map_err(|e| Box::new(e) as _)
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: "fail to open shards in cluster",
        })?;

    let topology = ctx
        .cluster
        .fetch_nodes()
        .await
        .map_err(|e| Box::new(e) as _)
        .with_context(|| ErrWithCause {
            code: StatusCode::Internal,
            msg: format!(
                "fail to get topology while opening shard, request:{:?}",
                request
            ),
        })?;

    let shard_info = tables_of_shard.shard_info;
    let default_catalog = ctx.default_catalog()?;
    let opts = OpenOptions {
        table_engine: ctx.table_engine,
    };

    for table in tables_of_shard.tables {
        let schema = default_catalog
            .schema_by_name(&table.schema_name)
            .map_err(|e| Box::new(e) as _)
            .with_context(|| ErrWithCause {
                code: StatusCode::Internal,
                msg: format!("fail to get schema of table, table_info:{:?}", table),
            })?
            .with_context(|| ErrNoCause {
                code: StatusCode::NotFound,
                msg: format!("schema of table is not found, table_info:{:?}", table),
            })?;

        let open_request = OpenTableRequest {
            catalog_name: ctx.catalog_manager.default_catalog_name().to_string(),
            schema_name: table.schema_name,
            schema_id: SchemaId::from(table.schema_id),
            table_name: table.name.clone(),
            table_id: TableId::new(table.id),
            engine: table_engine::ANALYTIC_ENGINE_TYPE.to_string(),
            shard_id: shard_info.id,
            cluster_version: topology.cluster_topology_version,
        };
        schema
            .open_table(open_request.clone(), opts.clone())
            .await
            .map_err(|e| Box::new(e) as _)
            .with_context(|| ErrWithCause {
                code: StatusCode::Internal,
                msg: format!("fail to open table, open_request:{:?}", open_request),
            })?
            .with_context(|| ErrNoCause {
                code: StatusCode::Internal,
                msg: format!("no table is opened, open_request:{:?}", open_request),
            })?;
    }

    Ok(())
}

async fn handle_close_shard(ctx: HandlerContext, request: CloseShardRequest) -> Result<()> {
    let tables_of_shard = ctx
        .cluster
        .close_shard(&request)
        .await
        .map_err(|e| Box::new(e) as _)
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: "fail to close shards in cluster",
        })?;

    let default_catalog = ctx.default_catalog()?;

    let opts = CloseOptions {
        table_engine: ctx.table_engine,
    };
    for table in tables_of_shard.tables {
        let schema = default_catalog
            .schema_by_name(&table.schema_name)
            .map_err(|e| Box::new(e) as _)
            .with_context(|| ErrWithCause {
                code: StatusCode::Internal,
                msg: format!("fail to get schema of table, table_info:{:?}", table),
            })?
            .with_context(|| ErrNoCause {
                code: StatusCode::NotFound,
                msg: format!("schema of table is not found, table_info:{:?}", table),
            })?;

        let close_request = CloseTableRequest {
            catalog_name: ctx.catalog_manager.default_catalog_name().to_string(),
            schema_name: table.schema_name,
            schema_id: SchemaId::from(table.schema_id),
            table_name: table.name.clone(),
            table_id: TableId::new(table.id),
            engine: table_engine::ANALYTIC_ENGINE_TYPE.to_string(),
        };
        schema
            .close_table(close_request.clone(), opts.clone())
            .await
            .map_err(|e| Box::new(e) as _)
            .with_context(|| ErrWithCause {
                code: StatusCode::Internal,
                msg: format!("fail to close table, close_request:{:?}", close_request),
            })?;
    }

    Ok(())
}

async fn handle_create_table_on_shard(
    ctx: HandlerContext,
    request: CreateTableOnShardRequest,
) -> Result<()> {
    ctx.cluster
        .create_table_on_shard(&request)
        .await
        .map_err(|e| Box::new(e) as _)
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: format!(
                "fail to create table on shard in cluster, req:{:?}",
                request
            ),
        })?;

    let topology = ctx
        .cluster
        .fetch_nodes()
        .await
        .map_err(|e| Box::new(e) as _)
        .with_context(|| ErrWithCause {
            code: StatusCode::Internal,
            msg: format!(
                "fail to get topology while creating table, request:{:?}",
                request
            ),
        })?;

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
    let table = request.table_info.context(ErrNoCause {
        code: StatusCode::BadRequest,
        msg: "table info is missing in the CreateTableOnShardRequest",
    })?;

    // Create the table by catalog manager afterwards.
    let default_catalog = ctx.default_catalog()?;

    let schema = default_catalog
        .schema_by_name(&table.schema_name)
        .map_err(|e| Box::new(e) as _)
        .with_context(|| ErrWithCause {
            code: StatusCode::Internal,
            msg: format!("fail to get schema of table, table_info:{:?}", table),
        })?
        .with_context(|| ErrNoCause {
            code: StatusCode::NotFound,
            msg: format!("schema of table is not found, table_info:{:?}", table),
        })?;

    let table_schema = SchemaEncoder::default()
        .decode(&request.encoded_schema)
        .map_err(|e| Box::new(e) as _)
        .context(ErrWithCause {
            code: StatusCode::BadRequest,
            msg: format!(
                "fail to decode encoded schema bytes, raw_bytes:{:?}",
                request.encoded_schema
            ),
        })?;

    let create_table_request = CreateTableRequest {
        catalog_name: ctx.catalog_manager.default_catalog_name().to_string(),
        schema_name: table.schema_name,
        schema_id: SchemaId::from_u32(table.schema_id),
        table_name: table.name,
        table_schema,
        engine: request.engine,
        options: request.options,
        state: TableState::Stable,
        shard_id: shard_info.id,
        cluster_version: topology.cluster_topology_version,
        partition_info: None,
    };
    let create_opts = CreateOptions {
        table_engine: ctx.table_engine,
        create_if_not_exists: request.create_if_not_exist,
    };

    schema
        .create_table(create_table_request.clone(), create_opts)
        .await
        .map_err(|e| Box::new(e) as _)
        .with_context(|| ErrWithCause {
            code: StatusCode::Internal,
            msg: format!(
                "fail to create table with request:{:?}",
                create_table_request
            ),
        })?;

    Ok(())
}

async fn handle_drop_table_on_shard(
    ctx: HandlerContext,
    request: DropTableOnShardRequest,
) -> Result<()> {
    ctx.cluster
        .drop_table_on_shard(&request)
        .await
        .map_err(|e| Box::new(e) as _)
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: format!("fail to drop table on shard in cluster, req:{:?}", request),
        })?;

    let table = request.table_info.context(ErrNoCause {
        code: StatusCode::BadRequest,
        msg: "table info is missing in the CreateTableOnShardRequest",
    })?;

    // Drop the table by catalog manager afterwards.
    let default_catalog = ctx.default_catalog()?;

    let schema = default_catalog
        .schema_by_name(&table.schema_name)
        .map_err(|e| Box::new(e) as _)
        .with_context(|| ErrWithCause {
            code: StatusCode::Internal,
            msg: format!("fail to get schema of table, table_info:{:?}", table),
        })?
        .with_context(|| ErrNoCause {
            code: StatusCode::NotFound,
            msg: format!("schema of table is not found, table_info:{:?}", table),
        })?;

    let drop_table_request = DropTableRequest {
        catalog_name: ctx.catalog_manager.default_catalog_name().to_string(),
        schema_name: table.schema_name,
        schema_id: SchemaId::from_u32(table.schema_id),
        table_name: table.name,
        // FIXME: the engine type should not use the default one.
        engine: ANALYTIC_ENGINE_TYPE.to_string(),
    };
    let drop_opts = DropOptions {
        table_engine: ctx.table_engine,
    };

    schema
        .drop_table(drop_table_request.clone(), drop_opts)
        .await
        .map_err(|e| Box::new(e) as _)
        .with_context(|| ErrWithCause {
            code: StatusCode::Internal,
            msg: format!("fail to drop table with request:{:?}", drop_table_request),
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
