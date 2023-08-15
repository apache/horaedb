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

//! Request handlers

pub mod admin;
mod error;

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
