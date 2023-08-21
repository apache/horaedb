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

//! Context for instance

use std::{fmt, sync::Arc};

use table_engine::engine::EngineRuntimes;

use crate::{sst::meta_data::cache::MetaCacheRef, Config};

/// Context for instance open
pub struct OpenContext {
    /// Engine config
    pub config: Config,

    /// Background job runtime
    pub runtimes: Arc<EngineRuntimes>,

    /// Sst meta data cache.
    pub meta_cache: Option<MetaCacheRef>,
}

impl fmt::Debug for OpenContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenContext")
            .field("config", &self.config)
            .finish()
    }
}
