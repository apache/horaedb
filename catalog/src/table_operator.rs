use std::result;

use table_engine::{ANALYTIC_ENGINE_TYPE, engine::{OpenShardRequest, CloseShardRequest, TableEngineRef}, table::TableRef};

use crate::{manager::ManagerRef, CatalogRef, schema::{OpenOptions, NameRef, SchemaRef, CloseOptions, CloseTableRequest, OpenTableRequest, CreateOptions, CreateTableRequest, DropTableRequest, DropOptions}};

use crate::Result;
/// Table operator 
/// 
/// Encapsulate all operation about tables rather than placing them everywhere(e.g. duplicated codes in `Interpreters` and `MetaEventService`). 
pub struct TableOperator {
    catalog_manager: ManagerRef,
}

impl TableOperator {
    pub fn open_tables_of_shard(&self, request: OpenShardRequest, opts: OpenOptions) {
        let table_engine = opts.table_engine;
        let shard_id = request.shard_id;

        // Generate open requests.
        let table_infos = request.table_infos;
        let schemas_and_requests = table_infos
            .into_iter()
            .map(|table| {
                let schema_res = self.schema_by_name(&table.catalog_name, &table.schema_name);

                schema_res.map(|schema| {
                    let request = OpenTableRequest {
                        catalog_name: table.catalog_name,
                        schema_name: table.schema_name,
                        schema_id: SchemaId::from(table.schema_id),
                        table_name: table.table_name.clone(),
                        table_id: table.table_id,
                        engine: request.engine.clone(),
                        shard_id: request.shard_id,
                        cluster_version: 0,
                    };

                    (schema, request)
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let (schemas, requests): (Vec<_>, Vec<_>) = schemas_and_requests.into_iter().unzip();

        // Open tables by table engine.
        // TODO: add the `open_shard` method into table engine.
        let open_res = open_tables_of_shard(opts.table_engine.clone(), requests).await;

        // Check and register successful opened table into schema.
        let mut success_count = 0_u32;
        let mut no_table_count = 0_u32;
        let mut open_err_count = 0_u32;

        for (schema, open_res) in schemas.into_iter().zip(open_res.into_iter()) {
            match open_res {
                Ok(Some(table)) => {
                    schema.register_table(table);
                    success_count += 1;
                }
                Ok(None) => {
                    no_table_count += 1;
                }
                // Has printed error log for it.
                Err(_) => {
                    open_err_count += 1;
                }
            }
        }

        info!(
            "Open shard finish, shard id:{}, cost:{}ms, successful count:{}, no table is opened count:{}, open error count:{}",
            shard_id,
            instant.saturating_elapsed().as_millis(),
            success_count,
            no_table_count,
            open_err_count
        );

        if no_table_count == 0 && open_err_count == 0 {
            Ok(())
        } else {
            Err(Error::ErrNoCause {
                code: StatusCode::Internal,
                msg: format!(
                    "Failed to open shard, some tables open failed, no table is shard id:{}, opened count:{}, open error count:{}",
                    shard_id, no_table_count, open_err_count
                ),
            })
        }

        Ok(())
    }

    async fn close_tables_of_shard(&self, request: CloseShardRequest, opts: CloseOptions) -> Result<()> {
        let table_engine = opts.table_engine;

        // Generate open requests.
        let table_infos = request.table_infos;
        let schemas_and_requests = table_infos
            .into_iter()
            .filter(|table| {
                let schema_res = self.schema_by_name(&table.catalog_name, &table.schema_name);

                schema_res.map(|schema| {
                    let request = CloseTableRequest {
                        catalog_name: table.catalog_name,
                        schema_name: table.schema_name,
                        schema_id: SchemaId::from(table.schema_id),
                        table_name: table.table_name.clone(),
                        table_id: table.table_id,
                        engine: request.engine.clone(),
                    };

                    (schema, request)
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let (schemas, requests): (Vec<_>, Vec<_>) = schemas_and_requests.into_iter().unzip();
        
        //  Close tables by table engine.
        // TODO: add the `close_shard` method into table engine.
        let results = close_tables_of_shard(table_engine, requests).await;

        // Check and unregister successful closed table from schema.
        let mut success_count = 0_u32;
        let mut close_err_count = 0_u32;

        for (schema, result) in schemas.into_iter().zip(results.into_iter()) {
            match result {
                Ok(()) => {
                    schema.register_table(table);
                    success_count += 1;
                },
                Err(_) => {
                    close_err_count += 1;
                }
            }
        }

        info!(
            "Close shard finished, shard id:{}, cost:{}ms, success_count:{}, close_err_count:{}",
            shard_id,
            instant.saturating_elapsed().as_millis(),
            success_count,
            open_err_count
        );

        if close_err_count == 0 {
            Ok(())
        } else {
            Err(Error::ErrNoCause {
                code: StatusCode::Internal,
                msg: format!(
                    "Failed to close shard, some tables open failed, no table is shard id:{}, close_err_count:{}",
                    shard_id, close_err_count
                ),
            })
        }

        Ok(())
    }

    async fn open_table_on_shard(&self, request: OpenTableRequest, opts: OpenOptions) {
        let table_engine = opts.table_engine;
        let schema = self.schema_by_name(&request.catalog_name, &request.schema_name).unwrap();

        let table = table_engine.open_table(request).await.unwrap().unwrap();
        schema.register_table(table);

        Ok(())
    }

    async fn close_table_on_shard(&self, request: CloseTableRequest, opts: CloseOptions) {
        let table_engine = opts.table_engine;
        let schema = self.schema_by_name(&request.catalog_name, &request.schema_name).unwrap();
        let table_name = request.table_name.clone();

        table_engine.close_table(request).await.unwrap().unwrap();
        schema.unregister_table(&table);

        Ok(())
    }

    async fn create_table_on_shard(&self, request: CreateTableRequest, opts: CreateOptions) {
        let table_engine = opts.table_engine;
        let schema = self.schema_by_name(&request.catalog_name, &request.schema_name).unwrap();

        table_engine.create_table(request).await.unwrap().unwrap();
        schema.register_table(&table);

        Ok(())
    }

    async fn drop_table_on_shard(&self, request: DropTableRequest, opts: DropOptions) {
        let table_engine = opts.table_engine;
        let schema = self.schema_by_name(&request.catalog_name, &request.schema_name).unwrap();
        let table_name = request.table_name.clone();

        table_engine.drop_table(request).await.unwrap().unwrap();
        schema.unregister_table(&table_name);

        Ok(())
    }

    fn schema_by_name(&self, catalog_name: &str, schema_name:&str) -> Result<SchemaRef> {
        let catalog = self
            .catalog_manager
            .catalog_by_name(catalog_name).unwrap().unwrap();
            // .box_err()
            // .context(ErrWithCause {
            //     code: StatusCode::Internal,
            //     msg: "fail to get default catalog",
            // })?
            // .context(ErrNoCause {
            //     code: StatusCode::NotFound,
            //     msg: "default catalog is not found",
            // })?;

         Ok(catalog.schema_by_name(schema_name).unwrap().unwrap())
    }
}

async fn open_tables_of_shard(
    table_engine: TableEngineRef,
    open_requests: Vec<OpenTableRequest>,
) -> Vec<table_engine::engine::Result<Option<TableRef>>> {
    if open_requests.is_empty() {
        return Vec::new();
    }

    let mut open_results = Vec::with_capacity(open_requests.len());
    for request in open_requests {
        let result = table_engine.open_table(request.clone()).await
        .map_err(|e| {
            error!("Failed to open table, open_request:{request:?}, err:{e}");
            e
        })
        .map(|table_opt| {
            if table_opt.is_none() {
                error!("Table engine returns none when opening table, open_request:{request:?}");
            }
            table_opt
        });

        open_results.push(result);
    }

    open_results
}

async fn close_tables_of_shard(
    table_engine: TableEngineRef,
    close_requests: Vec<CloseTableRequest>,
) -> Vec<table_engine::engine::Result<()>> {
    if close_requests.is_empty() {
        return Vec::new();
    }

    let mut close_results = Vec::with_capacity(close_requests.len());
    for request in close_requests {
        let result = table_engine.open_table(request.clone()).await
        .map_err(|e| {
            error!("Failed to close table, close_request:{request:?}, err:{e}");
            e
        });

        close_results.push(result);
    }

    close_results
}
