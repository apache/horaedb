use ceresdbproto::storage::{Route, RouteRequest};
use common_util::error::BoxError;
use query_engine::executor::Executor as QueryExecutor;
use snafu::ResultExt;

use crate::proxy::{
    error::{self, Internal, Result},
    Context, Proxy,
};

pub struct RouteResponse {
    pub routes: Vec<Route>,
}

impl<Q: QueryExecutor + 'static> Proxy<Q> {
    pub async fn handle_route(&self, _ctx: Context, req: RouteRequest) -> Result<RouteResponse> {
        let routes = self.router.route(req).await.box_err().context(Internal {
            msg: "fail to route".to_string(),
        })?;
        Ok(RouteResponse { routes })
    }
}
