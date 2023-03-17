// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! route request handler

use std::{ time::Instant};

use ceresdbproto::storage::{Route, RouteRequest};
use common_util::time::InstantExt;
use log::info;
use serde::{Serialize, Serializer};
use serde::ser::{SerializeMap, SerializeSeq};
use snafu::{ensure};

use crate::handlers::{
    error::{
        RouteHandler
    },
    prelude::*,
};

#[serde(rename_all = "snake_case")]
pub struct Response {
    pub routes: Vec<Route>,
}

impl Serialize for Response {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
    { let mut seq = serializer.serialize_seq(Some(self.routes.len()))?;
        for route in self.routes{
            let endpoint = route.endpoint.unwrap();
            let tup = (route.table.clone(), format!("{}:{}",endpoint.ip,endpoint.port));
            seq.serialize_element(&tup)?;
        }
        seq.end()
    }
}

pub async fn handle_route<Q: QueryExecutor + 'static>(
    ctx: &RequestContext,
    instance: InstanceRef<Q>,
    table: String,
) -> Result<Response> {
    ensure!(!table.is_empty());
    let begin_instant = Instant::now();
    let deadline = ctx.timeout.map(|t| begin_instant + t);

    info!("Route handler try to find route of table:{table}");

    let req = RouteRequest {
        context: Some(ceresdbproto::storage::RequestContext{
            database: ctx.schema.clone()
        }),
        tables: vec![table],
    };
    let routes = ctx.router.route(req).await.context(RouteHandler{
        table: table.clone()
    })?;

    info!(
        "Route handler finished, cost:{}ms, table:{}",
        begin_instant.saturating_elapsed().as_millis(),
        table
    );

    Ok(Response{routes} )
}
