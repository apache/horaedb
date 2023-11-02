// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! This module implements [put][1], [query][2] for OpenTSDB
//! [1]: http://opentsdb.net/docs/build/html/api_http/put.html
//! [2]: http://opentsdb.net/docs/build/html/api_http/query/index.html

use std::{collections::HashMap, time::Instant};

use ceresdbproto::storage::{
    RequestContext as GrpcRequestContext, WriteRequest as GrpcWriteRequest,
};
use common_types::{datum::DatumKind, record_batch::RecordBatch, schema::RecordSchema};
use futures::{stream::FuturesOrdered, StreamExt};
use generic_error::BoxError;
use http::StatusCode;
use interpreters::interpreter::Output;
use logger::{debug, info};
use query_frontend::{
    frontend::{Context as SqlContext, Frontend},
    opentsdb::types::QueryRequest,
    provider::CatalogMetaProvider,
};
use snafu::{ensure, OptionExt, ResultExt};

use self::types::QueryResponse;
use crate::{
    context::RequestContext,
    error::{ErrNoCause, ErrWithCause, InternalNoCause, Result},
    metrics::HTTP_HANDLER_COUNTER_VEC,
    opentsdb::types::{convert_put_request, PutRequest, PutResponse},
    Context, Proxy,
};

pub mod types;

impl Proxy {
    pub async fn handle_opentsdb_put(
        &self,
        ctx: RequestContext,
        req: PutRequest,
    ) -> Result<PutResponse> {
        let write_table_requests = convert_put_request(req)?;
        let num_rows: usize = write_table_requests
            .iter()
            .map(|req| {
                req.entries
                    .iter()
                    .map(|e| e.field_groups.len())
                    .sum::<usize>()
            })
            .sum();

        let table_request = GrpcWriteRequest {
            context: Some(GrpcRequestContext {
                database: ctx.schema.clone(),
            }),
            table_requests: write_table_requests,
        };
        let proxy_context = Context::new(ctx.timeout, None);

        match self
            .handle_write_internal(proxy_context, table_request)
            .await
        {
            Ok(result) => {
                if result.failed != 0 {
                    HTTP_HANDLER_COUNTER_VEC.write_failed.inc();
                    HTTP_HANDLER_COUNTER_VEC
                        .write_failed_row
                        .inc_by(result.failed as u64);
                    ErrNoCause {
                        code: StatusCode::INTERNAL_SERVER_ERROR,
                        msg: format!("fail to write storage, failed rows:{:?}", result.failed),
                    }
                    .fail()?;
                }

                debug!(
                    "OpenTSDB write finished, catalog:{}, schema:{}, result:{result:?}",
                    ctx.catalog, ctx.schema
                );

                Ok(())
            }
            Err(e) => {
                HTTP_HANDLER_COUNTER_VEC.write_failed.inc();
                HTTP_HANDLER_COUNTER_VEC
                    .write_failed_row
                    .inc_by(num_rows as u64);
                Err(e)
            }
        }
    }

    pub async fn handle_opentsdb_query(
        &self,
        ctx: RequestContext,
        req: QueryRequest,
    ) -> Result<Vec<QueryResponse>> {
        let request_id = ctx.request_id;
        let begin_instant = Instant::now();
        let deadline = ctx.timeout.map(|t| begin_instant + t);

        info!(
            "Opentsdb query handler try to process request, request_id:{}, request:{:?}",
            request_id, req
        );

        let provider = CatalogMetaProvider {
            manager: self.instance.catalog_manager.clone(),
            default_catalog: &ctx.catalog,
            default_schema: &ctx.schema,
            function_registry: &*self.instance.function_registry,
        };
        let frontend = Frontend::new(provider, self.instance.dyn_config.fronted.clone());
        let sql_ctx = SqlContext::new(request_id, deadline);

        let opentsdb_plan = frontend
            .opentsdb_query_to_plan(&sql_ctx, req)
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::BAD_REQUEST,
                msg: "Failed to build plan",
            })?;

        for plan in &opentsdb_plan.plans {
            self.instance
                .limiter
                .try_limit(&plan.plan)
                .box_err()
                .context(ErrWithCause {
                    code: StatusCode::INTERNAL_SERVER_ERROR,
                    msg: "Query is blocked",
                })?;
        }

        let mut futures = FuturesOrdered::new();
        for plan in opentsdb_plan.plans {
            let one_resp = async {
                let output = self
                    .execute_plan(request_id, &ctx.catalog, &ctx.schema, plan.plan, deadline)
                    .await?;

                convert_output_to_response(
                    output,
                    plan.metric,
                    plan.field_col_name,
                    plan.timestamp_col_name,
                    plan.tags,
                    plan.aggregated_tags,
                )
            };

            futures.push_back(one_resp);
        }

        let resp = futures.collect::<Vec<_>>().await;
        let resp = resp.into_iter().collect::<Result<Vec<_>>>()?;
        let resp = resp.into_iter().flatten().collect();

        Ok(resp)
    }
}

fn convert_output_to_response(
    output: Output,
    metric: String,
    field_col_name: String,
    timestamp_col_name: String,
    tags: Vec<String>,
    aggregated_tags: Vec<String>,
) -> Result<Vec<QueryResponse>> {
    let records = match output {
        Output::Records(records) => records,
        Output::AffectedRows(_) => {
            return InternalNoCause {
                msg: "output in opentsdb query should not be affected rows",
            }
            .fail()
        }
    };

    let mut converter = match records.first() {
        None => {
            return Ok(Vec::new());
        }
        Some(batch) => {
            let record_schema = batch.schema();
            QueryConverter::try_new(
                record_schema,
                metric,
                &timestamp_col_name,
                &field_col_name,
                tags,
                aggregated_tags,
            )?
        }
    };

    for record in records {
        converter.add_batch(record)?;
    }

    Ok(converter.finish())
}

struct QueryConverter {
    timestamp_idx: usize,
    value_idx: usize,
    metric: String,
    // (column_name, index)
    tags_idx: Vec<(String, usize)>,
    tags: HashMap<String, HashMap<String, String>>,
    values: HashMap<String, Vec<(String, f64)>>,
    aggregated_tags: Vec<String>,

    resp: Vec<QueryResponse>,
}

impl QueryConverter {
    fn try_new(
        schema: &RecordSchema,
        metric: String,
        timestamp_col_name: &str,
        field_col_name: &str,
        tags: Vec<String>,
        aggregated_tags: Vec<String>,
    ) -> Result<Self> {
        let timestamp_idx = schema
            .index_of(timestamp_col_name)
            .context(InternalNoCause {
                msg: "Timestamp column is missing in query response",
            })?;
        let value_idx = schema.index_of(field_col_name).context(InternalNoCause {
            msg: "Value column is missing in query response",
        })?;
        let tags_idx = tags
            .iter()
            .map(|tag| {
                let column = schema.column_by_name(tag);
                ensure!(
                    column.is_some(),
                    InternalNoCause {
                        msg: format!("Tag can not be find in schema, tag:{}", tag)
                    }
                );
                let column = column.unwrap();
                ensure!(
                    matches!(column.data_type, DatumKind::String),
                    InternalNoCause {
                        msg: format!("Tag must be string type, current:{}", column.data_type)
                    }
                );
                let index = schema.index_by_name(tag).unwrap();
                Ok((column.name.to_string(), index))
            })
            .collect::<Result<Vec<_>>>()?;

        ensure!(
            schema.column(timestamp_idx).data_type.is_timestamp(),
            InternalNoCause {
                msg: format!(
                    "Timestamp wrong type, current:{}",
                    schema.column(timestamp_idx).data_type
                )
            }
        );
        ensure!(
            schema.column(value_idx).data_type.is_f64_castable(),
            InternalNoCause {
                msg: format!(
                    "Value must be f64 compatible type, current:{}",
                    schema.column(value_idx).data_type
                )
            }
        );

        Ok(QueryConverter {
            timestamp_idx,
            value_idx,
            metric,
            tags_idx,
            tags: Default::default(),
            values: Default::default(),
            aggregated_tags,
            resp: Vec::new(),
        })
    }

    fn add_batch(&mut self, record_batch: RecordBatch) -> Result<()> {
        let row_num = record_batch.num_rows();
        for row_idx in 0..row_num {
            let mut tags = HashMap::with_capacity(self.tags.len());
            let mut tags_key = String::new();
            for (tag_key, index) in self.tags_idx.iter() {
                let tag_value = record_batch
                    .column(*index)
                    .datum(row_idx)
                    .as_str()
                    .unwrap()
                    .to_string();
                tags_key += &tag_value;
                tags.insert(tag_key.clone(), tag_value);
            }
            let timestamp = record_batch
                .column(self.timestamp_idx)
                .datum(row_idx)
                .as_timestamp()
                .unwrap()
                .as_i64()
                .to_string();
            let value = record_batch
                .column(self.value_idx)
                .datum(row_idx)
                .as_f64()
                .unwrap();

            if self.tags.contains_key(&tags_key) {
                let values = self.values.get_mut(&tags_key).unwrap();
                values.push((timestamp, value));
            } else {
                self.tags.insert(tags_key.clone(), tags);
                self.values.insert(tags_key, vec![(timestamp, value)]);
            }
        }
        Ok(())
    }

    fn finish(mut self) -> Vec<QueryResponse> {
        for (key, tags) in self.tags {
            let values = self.values.get(&key).unwrap().clone();
            let mut dps = HashMap::with_capacity(values.len());
            for (time, value) in values {
                dps.insert(time, value);
            }

            let resp = QueryResponse {
                metric: self.metric.clone(),
                tags,
                aggregated_tags: self.aggregated_tags.clone(),
                dps,
            };
            self.resp.push(resp);
        }

        self.resp
    }
}
