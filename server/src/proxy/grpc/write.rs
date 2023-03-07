use std::time::Instant;
use ceresdbproto::storage::{RouteRequest, RouteResponse, WriteRequest, WriteResponse};
use log::debug;
use common_types::request_id::RequestId;
use crate::proxy::{Context, Proxy};
use crate::proxy::error::Error::Internal;
use crate::proxy::error::Result;

impl<Q: QueryExecutor + 'static> Proxy<Q> {
    pub(crate) async fn handle_write<Q: QueryExecutor + 'static>(
        ctx: Context,
        req: WriteRequest,
    ) -> Result<WriteResponse> {
        let request_id = RequestId::next_id();
        let begin_instant = Instant::now();
        let deadline = ctx.timeout.map(|t| begin_instant + t);
        let catalog = ctx.catalog();
        let req_ctx = req.context.unwrap();
        let schema = req_ctx.database;
        let schema_config = ctx
            .schema_config_provider
            .schema_config(&schema)
            .box_err()
            .with_context(|| Internal {
                msg: format!("fail to fetch schema config, schema_name:{schema}"),
            })?;

        debug!(
        "Grpc handle write begin, catalog:{}, schema:{}, request_id:{}, first_table:{:?}, num_tables:{}",
        catalog,
        schema,
        request_id,
        req.table_requests
            .first()
            .map(|m| (&m.table, &m.tag_names, &m.field_names)),
        req.table_requests.len(),
    );

        let plan_vec = write_request_to_insert_plan(
            request_id,
            catalog,
            &schema,
            ctx.instance.clone(),
            req.table_requests,
            schema_config,
            deadline,
        )
            .await?;

        let mut success = 0;
        for insert_plan in plan_vec {
            success += execute_plan(
                request_id,
                catalog,
                &schema,
                ctx.instance.clone(),
                insert_plan,
                deadline,
            )
                .await?;
        }

        let resp = WriteResponse {
            header: Some(error::build_ok_header()),
            success: success as u32,
            failed: 0,
        };

        debug!(
        "Grpc handle write finished, catalog:{}, schema:{}, resp:{:?}",
        catalog, schema, resp
    );

        Ok(resp)
    }
}

pub async fn write_request_to_insert_plan<Q: QueryExecutor + 'static>(
    request_id: RequestId,
    catalog: &str,
    schema: &str,
    instance: InstanceRef<Q>,
    table_requests: Vec<WriteTableRequest>,
    schema_config: Option<&SchemaConfig>,
    deadline: Option<Instant>,
) -> Result<Vec<InsertPlan>> {
    let mut plan_vec = Vec::with_capacity(table_requests.len());

    for write_table_req in table_requests {
        let table_name = &write_table_req.table;
        let mut table = try_get_table(catalog, schema, instance.clone(), table_name)?;

        if table.is_none() {
            // TODO: remove this clone?
            let schema_config = schema_config.cloned().unwrap_or_default();
            create_table(
                request_id,
                catalog,
                schema,
                instance.clone(),
                &write_table_req,
                &schema_config,
                deadline,
            )
                .await?;
            // try to get table again
            table = try_get_table(catalog, schema, instance.clone(), table_name)?;
        }

        match table {
            Some(table) => {
                let plan = write_table_request_to_insert_plan(table, write_table_req)?;
                plan_vec.push(plan);
            }
            None => {
                return ErrNoCause {
                    code: StatusCode::BAD_REQUEST,
                    msg: format!("Table not found, schema:{schema}, table:{table_name}"),
                }
                    .fail();
            }
        }
    }

    Ok(plan_vec)
}

pub async fn execute_plan<Q: QueryExecutor + 'static>(
    request_id: RequestId,
    catalog: &str,
    schema: &str,
    instance: InstanceRef<Q>,
    insert_plan: InsertPlan,
    deadline: Option<Instant>,
) -> Result<usize> {
    debug!(
        "Grpc handle write table begin, table:{}, row_num:{}",
        insert_plan.table.name(),
        insert_plan.rows.num_rows()
    );
    let plan = Plan::Insert(insert_plan);

    instance
        .limiter
        .try_limit(&plan)
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::FORBIDDEN,
            msg: "Insert is blocked",
        })?;

    let interpreter_ctx = InterpreterContext::builder(request_id, deadline)
        // Use current ctx's catalog and schema as default catalog and schema
        .default_catalog_and_schema(catalog.to_string(), schema.to_string())
        .build();
    let interpreter_factory = Factory::new(
        instance.query_executor.clone(),
        instance.catalog_manager.clone(),
        instance.table_engine.clone(),
        instance.table_manipulator.clone(),
    );
    let interpreter = interpreter_factory
        .create(interpreter_ctx, plan)
        .box_err()
        .with_context(|| ErrWithCause {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            msg: "Failed to create interpreter",
        })?;

    interpreter
        .execute()
        .await
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            msg: "failed to execute interpreter",
        })
        .and_then(|output| match output {
            Output::AffectedRows(n) => Ok(n),
            Output::Records(_) => ErrNoCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "Invalid output type, expect AffectedRows, found Records",
            }
                .fail(),
        })
}