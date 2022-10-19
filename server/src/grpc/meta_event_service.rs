// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

// Meta event rpc service implementation.

use std::sync::Arc;

use async_trait::async_trait;
use catalog::schema::{
    CloseOptions, CreateOptions, CreateTableRequest, DropOptions, DropTableRequest, OpenOptions,
    OpenTableRequest,
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
use http::StatusCode;
use log::info;
use query_engine::executor::Executor as QueryExecutor;
use snafu::{OptionExt, ResultExt};
use table_engine::{
    engine::{CloseTableRequest, TableState},
    table::{SchemaId, TableId},
    ANALYTIC_ENGINE_TYPE,
};

use crate::{
    error::{ErrNoCause, ErrWithCause},
    grpc,
    instance::InstanceRef,
};

#[derive(Clone)]
pub struct MetaServiceImpl<Q: QueryExecutor + 'static> {
    pub cluster: ClusterRef,
    pub instance: InstanceRef<Q>,
    pub runtime: Arc<Runtime>,
}

#[async_trait]
impl<Q: QueryExecutor + 'static> MetaEventService for MetaServiceImpl<Q> {
    // TODO: use macro to remove the boilerplate codes.
    async fn open_shard(
        &self,
        request: tonic::Request<OpenShardRequest>,
    ) -> std::result::Result<tonic::Response<OpenShardResponse>, tonic::Status> {
        let cluster = self.cluster.clone();
        let catalog_manager = self.instance.catalog_manager.clone();
        let table_engine = self.instance.table_engine.clone();

        let handle = self.runtime.spawn(async move {
            // FIXME: Data race about the operations on the shards should be taken into
            // considerations.

            let request = request.into_inner();
            let tables_of_shard = cluster
                .open_shard(&request)
                .await
                .map_err(|e| Box::new(e) as _)
                .context(ErrWithCause {
                    code: StatusCode::INTERNAL_SERVER_ERROR,
                    msg: "fail to open shards in cluster",
                })?;

            let default_catalog_name = catalog_manager.default_catalog_name();
            let default_catalog = catalog_manager
                .catalog_by_name(default_catalog_name)
                .map_err(|e| Box::new(e) as _)
                .context(ErrWithCause {
                    code: StatusCode::INTERNAL_SERVER_ERROR,
                    msg: "fail to get default catalog",
                })?
                .context(ErrNoCause {
                    code: StatusCode::NOT_FOUND,
                    msg: "default catalog is not found",
                })?;

            let shard_info = tables_of_shard.shard_info;
            let opts = OpenOptions { table_engine };
            for table in tables_of_shard.tables {
                let schema = default_catalog
                    .schema_by_name(&table.schema_name)
                    .map_err(|e| Box::new(e) as _)
                    .with_context(|| ErrWithCause {
                        code: StatusCode::INTERNAL_SERVER_ERROR,
                        msg: format!("fail to get schema of table, table_info:{:?}", table),
                    })?
                    .with_context(|| ErrNoCause {
                        code: StatusCode::NOT_FOUND,
                        msg: format!("schema of table is not found, table_info:{:?}", table),
                    })?;

                let open_request = OpenTableRequest {
                    catalog_name: default_catalog_name.to_string(),
                    schema_name: table.schema_name,
                    schema_id: SchemaId::from(table.schema_id),
                    table_name: table.name.clone(),
                    table_id: TableId::new(table.id),
                    engine: table_engine::ANALYTIC_ENGINE_TYPE.to_string(),
                    shard_id: shard_info.id,
                };
                schema
                    .open_table(open_request.clone(), opts.clone())
                    .await
                    .map_err(|e| Box::new(e) as _)
                    .with_context(|| ErrWithCause {
                        code: StatusCode::INTERNAL_SERVER_ERROR,
                        msg: format!("fail to open table, open_request:{:?}", open_request),
                    })?
                    .with_context(|| ErrNoCause {
                        code: StatusCode::INTERNAL_SERVER_ERROR,
                        msg: format!("no table is opened, open_request:{:?}", open_request),
                    })?;
            }

            Ok(())
        });

        let res = handle
            .await
            .map_err(|e| Box::new(e) as _)
            .context(ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "fail to join task",
            });

        let mut resp = OpenShardResponse::default();
        match res {
            Ok(Ok(_)) => {
                resp.header = Some(grpc::build_ok_header());
            }
            Ok(Err(e)) | Err(e) => {
                resp.header = Some(grpc::build_err_header(e));
            }
        };

        Ok(tonic::Response::new(resp))
    }

    async fn close_shard(
        &self,
        request: tonic::Request<CloseShardRequest>,
    ) -> std::result::Result<tonic::Response<CloseShardResponse>, tonic::Status> {
        let cluster = self.cluster.clone();
        let catalog_manager = self.instance.catalog_manager.clone();
        let table_engine = self.instance.table_engine.clone();

        let handle = self.runtime.spawn(async move {
            let request = request.into_inner();
            let tables_of_shard = cluster
                .close_shard(&request)
                .await
                .map_err(|e| Box::new(e) as _)
                .context(ErrWithCause {
                    code: StatusCode::INTERNAL_SERVER_ERROR,
                    msg: "fail to close shards in cluster",
                })?;

            let default_catalog_name = catalog_manager.default_catalog_name();
            let default_catalog = catalog_manager
                .catalog_by_name(default_catalog_name)
                .map_err(|e| Box::new(e) as _)
                .context(ErrWithCause {
                    code: StatusCode::INTERNAL_SERVER_ERROR,
                    msg: "fail to get default catalog",
                })?
                .context(ErrNoCause {
                    code: StatusCode::NOT_FOUND,
                    msg: "default catalog is not found",
                })?;

            let opts = CloseOptions { table_engine };
            for table in tables_of_shard.tables {
                let schema = default_catalog
                    .schema_by_name(&table.schema_name)
                    .map_err(|e| Box::new(e) as _)
                    .with_context(|| ErrWithCause {
                        code: StatusCode::INTERNAL_SERVER_ERROR,
                        msg: format!("fail to get schema of table, table_info:{:?}", table),
                    })?
                    .with_context(|| ErrNoCause {
                        code: StatusCode::NOT_FOUND,
                        msg: format!("schema of table is not found, table_info:{:?}", table),
                    })?;

                let close_request = CloseTableRequest {
                    catalog_name: default_catalog_name.to_string(),
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
                        code: StatusCode::INTERNAL_SERVER_ERROR,
                        msg: format!("fail to close table, close_request:{:?}", close_request),
                    })?;
            }

            Ok(())
        });

        let res = handle
            .await
            .map_err(|e| Box::new(e) as _)
            .context(ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "fail to join task",
            });

        let mut resp = CloseShardResponse::default();
        match res {
            Ok(Ok(_)) => {
                resp.header = Some(grpc::build_ok_header());
            }
            Ok(Err(e)) | Err(e) => {
                resp.header = Some(grpc::build_err_header(e));
            }
        };

        Ok(tonic::Response::new(resp))
    }

    async fn create_table_on_shard(
        &self,
        request: tonic::Request<CreateTableOnShardRequest>,
    ) -> std::result::Result<tonic::Response<CreateTableOnShardResponse>, tonic::Status> {
        let cluster = self.cluster.clone();
        let catalog_manager = self.instance.catalog_manager.clone();
        let table_engine = self.instance.table_engine.clone();

        let handle = self.runtime.spawn(async move {
            let request = request.into_inner();
            cluster
                .create_table_on_shard(&request)
                .await
                .map_err(|e| Box::new(e) as _)
                .context(ErrWithCause {
                    code: StatusCode::INTERNAL_SERVER_ERROR,
                    msg: format!(
                        "fail to create table on shard in cluster, req:{:?}",
                        request
                    ),
                })?;

            let shard_info = request
                .update_shard_info
                .context(ErrNoCause {
                    code: StatusCode::BAD_REQUEST,
                    msg: "update shard info is missing in the CreateTableOnShardRequest",
                })?
                .curr_shard_info
                .context(ErrNoCause {
                    code: StatusCode::BAD_REQUEST,
                    msg: "current shard info is missing ine CreateTableOnShardRequest",
                })?;
            let table = request.table_info.context(ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: "table info is missing in the CreateTableOnShardRequest",
            })?;

            // Create the table by catalog manager afterwards.
            let default_catalog_name = catalog_manager.default_catalog_name();
            let default_catalog = catalog_manager
                .catalog_by_name(default_catalog_name)
                .map_err(|e| Box::new(e) as _)
                .context(ErrWithCause {
                    code: StatusCode::INTERNAL_SERVER_ERROR,
                    msg: "fail to get default catalog",
                })?
                .context(ErrNoCause {
                    code: StatusCode::NOT_FOUND,
                    msg: "default catalog is not found",
                })?;

            let schema = default_catalog
                .schema_by_name(&table.schema_name)
                .map_err(|e| Box::new(e) as _)
                .with_context(|| ErrWithCause {
                    code: StatusCode::INTERNAL_SERVER_ERROR,
                    msg: format!("fail to get schema of table, table_info:{:?}", table),
                })?
                .with_context(|| ErrNoCause {
                    code: StatusCode::NOT_FOUND,
                    msg: format!("schema of table is not found, table_info:{:?}", table),
                })?;

            let table_schema = SchemaEncoder::default()
                .decode(&request.encoded_schema)
                .map_err(|e| Box::new(e) as _)
                .context(ErrWithCause {
                    code: StatusCode::BAD_REQUEST,
                    msg: format!(
                        "fail to decode encoded schema bytes, raw_bytes:{:?}",
                        request.encoded_schema
                    ),
                })?;
            let create_table_request = CreateTableRequest {
                catalog_name: default_catalog_name.to_string(),
                schema_name: table.schema_name,
                schema_id: SchemaId::from_u32(table.schema_id),
                table_name: table.name,
                table_schema,
                engine: request.engine,
                options: request.options,
                state: TableState::Stable,
                shard_id: shard_info.id,
            };
            let create_opts = CreateOptions {
                table_engine,
                create_if_not_exists: request.create_if_not_exist,
            };

            schema
                .create_table(create_table_request.clone(), create_opts)
                .await
                .map_err(|e| Box::new(e) as _)
                .with_context(|| ErrWithCause {
                    code: StatusCode::INTERNAL_SERVER_ERROR,
                    msg: format!(
                        "fail to create table with request:{:?}",
                        create_table_request
                    ),
                })
        });

        let res = handle
            .await
            .map_err(|e| Box::new(e) as _)
            .context(ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "fail to join task",
            });

        let mut resp = CreateTableOnShardResponse::default();
        match res {
            Ok(Ok(_)) => {
                resp.header = Some(grpc::build_ok_header());
            }
            Ok(Err(e)) | Err(e) => {
                resp.header = Some(grpc::build_err_header(e));
            }
        };

        Ok(tonic::Response::new(resp))
    }

    async fn drop_table_on_shard(
        &self,
        request: tonic::Request<DropTableOnShardRequest>,
    ) -> std::result::Result<tonic::Response<DropTableOnShardResponse>, tonic::Status> {
        let cluster = self.cluster.clone();
        let catalog_manager = self.instance.catalog_manager.clone();
        let table_engine = self.instance.table_engine.clone();

        let handle = self.runtime.spawn(async move {
            let request = request.into_inner();
            cluster
                .drop_table_on_shard(&request)
                .await
                .map_err(|e| Box::new(e) as _)
                .context(ErrWithCause {
                    code: StatusCode::INTERNAL_SERVER_ERROR,
                    msg: format!("fail to drop table on shard in cluster, req:{:?}", request),
                })?;

            let table = request.table_info.context(ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: "table info is missing in the CreateTableOnShardRequest",
            })?;

            // Drop the table by catalog manager afterwards.
            let default_catalog_name = catalog_manager.default_catalog_name();
            let default_catalog = catalog_manager
                .catalog_by_name(default_catalog_name)
                .map_err(|e| Box::new(e) as _)
                .context(ErrWithCause {
                    code: StatusCode::INTERNAL_SERVER_ERROR,
                    msg: "fail to get default catalog",
                })?
                .context(ErrNoCause {
                    code: StatusCode::NOT_FOUND,
                    msg: "default catalog is not found",
                })?;

            let schema = default_catalog
                .schema_by_name(&table.schema_name)
                .map_err(|e| Box::new(e) as _)
                .with_context(|| ErrWithCause {
                    code: StatusCode::INTERNAL_SERVER_ERROR,
                    msg: format!("fail to get schema of table, table_info:{:?}", table),
                })?
                .with_context(|| ErrNoCause {
                    code: StatusCode::NOT_FOUND,
                    msg: format!("schema of table is not found, table_info:{:?}", table),
                })?;

            let drop_table_request = DropTableRequest {
                catalog_name: default_catalog_name.to_string(),
                schema_name: table.schema_name,
                schema_id: SchemaId::from_u32(table.schema_id),
                table_name: table.name,
                // FIXME: the engine type should not use the default one.
                engine: ANALYTIC_ENGINE_TYPE.to_string(),
            };
            let drop_opts = DropOptions { table_engine };

            schema
                .drop_table(drop_table_request.clone(), drop_opts)
                .await
                .map_err(|e| Box::new(e) as _)
                .with_context(|| ErrWithCause {
                    code: StatusCode::INTERNAL_SERVER_ERROR,
                    msg: format!("fail to drop table with request:{:?}", drop_table_request),
                })
        });

        let res = handle
            .await
            .map_err(|e| Box::new(e) as _)
            .context(ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "fail to join task",
            });

        let mut resp = DropTableOnShardResponse::default();
        match res {
            Ok(Ok(_)) => {
                resp.header = Some(grpc::build_ok_header());
            }
            Ok(Err(e)) | Err(e) => {
                resp.header = Some(grpc::build_err_header(e));
            }
        };

        Ok(tonic::Response::new(resp))
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
