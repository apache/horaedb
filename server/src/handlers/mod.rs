// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Request handlers

pub mod admin;
pub mod error;
pub mod prom;
pub mod query;

mod prelude {
    pub use catalog::manager::Manager as CatalogManager;
    pub use query_engine::executor::Executor as QueryExecutor;
    pub use serde::{Deserialize, Serialize};
    pub use snafu::ResultExt;
    pub use warp::Filter;

    pub use crate::{
        context::RequestContext,
        handlers::error::{Error, Result},
        instance::InstanceRef,
    };
}
